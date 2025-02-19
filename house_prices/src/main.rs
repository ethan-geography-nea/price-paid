use chrono::{self, Datelike, NaiveDate};
use csv::Reader;
use serde;
use std::collections::HashMap;

#[derive(Debug, serde::Deserialize, Clone)]
struct UKHPIRecord {
    #[serde(rename = "Name")]
    region: String,
    #[serde(rename = "Pivotable date")]
    time: NaiveDate,
    #[serde(rename = "Average price All property types")]
    average_price_all: i32,
    #[serde(rename = "Average price Flats and maisonettes")]
    average_price_flats: i32,
    #[serde(rename = "House price index All property types")]
    hpi_all: f32,
    #[serde(rename = "House price index Flats and maisonettes")]
    hpi_flats: f32,
    #[serde(rename = "Percentage change (monthly) All property types")]
    percentage_change_monthly_all: Option<f32>,
    #[serde(rename = "Percentage change (yearly) All property types")]
    percentage_change_yearly_all: Option<f32>,
    #[serde(rename = "Percentage change (monthly) Flats and maisonettes")]
    percentage_change_monthly_flats: Option<f32>,
    #[serde(rename = "Percentage change (yearly) Flats and maisonettes")]
    percentage_change_yearly_flats: Option<f32>,
    #[serde(rename = "Sales volume")]
    volume: Option<f32>,
}

#[derive(Debug, serde::Deserialize, Clone)]
struct PPDSRecord {
    #[serde(rename = "deed_date")]
    date: NaiveDate,
    #[serde(rename = "saon")]
    flat_name: String,
    #[serde(rename = "paon")]
    building: String,
    #[serde(rename = "street")]
    estate: String,
    #[serde(rename = "price_paid")]
    price_paid: i32,
}

fn create_reference_mapping(filename: &str) -> HashMap<(i32, i32), UKHPIRecord> {
    let mut map = HashMap::new();
    let mut reader = Reader::from_path(filename).unwrap();

    for result in reader.deserialize::<UKHPIRecord>() {
        let r = format!("{result:?}");
        match result {
            Ok(n) => {
                let yr: i32 = n.time.year();
                let mth: i32 = n.time.month() as i32;
                map.insert((mth, yr), n);
            }
            Err(e) => {
                println!("{r}: {e:?}");
            }
        }
    }

    map
}

fn create_ppd_mapping(filename: &str) -> HashMap<(String, String), Vec<PPDSRecord>> {
    let mut map: HashMap<(String, String), Vec<PPDSRecord>> = HashMap::new();
    let mut reader = Reader::from_path(filename).unwrap();

    for result in reader.deserialize::<PPDSRecord>() {
        let r = format!("{result:?}");
        match result {
            Ok(n) => {
                if map.contains_key(&(n.flat_name.clone(), n.building.clone())) {
                    map.get_mut(&(n.flat_name.clone(), n.building.clone()))
                        .unwrap()
                        .push(n);
                } else {
                    map.insert((n.flat_name.clone(), n.building.clone()), vec![n]);
                }
            }
            Err(e) => {
                println!("{r}: {e:?}");
            }
        }
    }

    map
}

#[derive(Debug)]
struct OutputCSV {
    labels: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl OutputCSV {
    fn new() -> OutputCSV {
        OutputCSV {
            labels: vec![String::from("date")],
            rows: Vec::new(),
        }
    }

    fn add_entries(&mut self, flat_name: String, building: String, ppds: Vec<PPDSRecord>) {
        self.labels.push(format!("{flat_name}, {building}"));
        for row in &mut self.rows {
            row.push("".to_string());
        }

        for ppd in ppds {
            let mut new_row: Vec<String> = vec!["".to_string(); self.labels.len() - 1];
            new_row[0] = ppd.date.format("%Y-%m-01").to_string();
            new_row.push(format!("{}", ppd.price_paid));
            self.rows.push(new_row);
        }
    }

    fn add_entries_hdi<F>(
        &mut self,
        flat_name: String,
        building: String,
        ppds: Vec<UKHPIRecord>,
        price_to_use: F,
    ) where
        F: Fn(UKHPIRecord) -> i32,
    {
        self.labels.push(format!("{flat_name}, {building}"));
        for row in &mut self.rows {
            row.push("".to_string());
        }

        for ppd in ppds {
            let mut new_row: Vec<String> = vec!["".to_string(); self.labels.len() - 1];
            new_row[0] = ppd.time.format("%Y-%m-01").to_string();
            new_row.push(format!("{}", price_to_use(ppd)));
            self.rows.push(new_row);
        }
    }
}

/// Both `length_filter` and `date_distance_filter` **REJECT** datapoints that return `true`.
/// `number_to_return`: Some(`i32`). If some, returns the top n weighted by `length of time between first and last` * `number of sales` * `0.5`
fn filter_and_write<F, D>(
    ppd_map: HashMap<(String, String), Vec<PPDSRecord>>,
    reference_maps: Vec<HashMap<(i32, i32), UKHPIRecord>>,
    length_filter: F,
    date_distance_filter: D,
    number_to_return: Option<i32>,
    output_filepath: &str,
) where
    F: Fn(usize) -> bool,
    D: Fn(i64) -> bool,
{
    let mut output = OutputCSV::new();

    let mut min_date: Option<NaiveDate> = None;
    let mut max_date: Option<NaiveDate> = None;

    #[derive(Clone)]
    struct Datapoint {
        score: f64,
        flat: String,
        building: String,
        first: NaiveDate,
        last: NaiveDate,
        records: Vec<PPDSRecord>,
    }

    let mut datapoints_to_process = Vec::new();

    for (k, v) in ppd_map {
        if length_filter(v.len()) {
            continue;
        }

        let mut v_copy = v.clone();
        v_copy.sort_by(|a, b| a.date.cmp(&b.date));
        let first = v_copy.first().unwrap().date;
        let last = v_copy.last().unwrap().date;

        if date_distance_filter((last - first).num_days()) {
            continue;
        }

        let score = (last - first).num_days() as f64 * v.len() as f64 * 0.5f64;

        datapoints_to_process.push(Datapoint {
            score,
            flat: k.0,
            building: k.1,
            first,
            last,
            records: v_copy,
        });
    }

    datapoints_to_process.sort_by(|a, b| b.score.total_cmp(&a.score));

    datapoints_to_process = match number_to_return {
        Some(n) => datapoints_to_process.into_iter().take(n as usize).collect(),
        None => datapoints_to_process,
    };

    for dp in datapoints_to_process {
        min_date = match min_date.clone() {
            None => Some(dp.first),
            Some(n) => Some(n.min(dp.first)),
        };

        max_date = match max_date.clone() {
            None => Some(dp.last),
            Some(n) => Some(n.max(dp.last)),
        };

        // println!("{k:?} : {} {}d", v.len(), (last - first).num_days());

        output.add_entries(dp.flat, dp.building, dp.records);
    }

    println!("{output:?}");

    for ref_map in reference_maps {
        let mut records = vec![];
        for (_, dp) in ref_map {
            if (min_date.unwrap() <= dp.time) && (dp.time <= max_date.unwrap()) {
                records.push(dp);
            }
        }

        output.add_entries_hdi(
            records.first().unwrap().region.clone(),
            "all sales average".to_string(),
            records.clone(),
            |x| x.average_price_all,
        );

        output.add_entries_hdi(
            records.first().unwrap().region.clone(),
            "flats average".to_string(),
            records,
            |x| x.average_price_flats,
        );
    }

    let mut writer = csv::Writer::from_path(output_filepath).unwrap();
    _ = writer.write_record(output.labels);
    for row in output.rows {
        _ = writer.write_record(row);
    }

    _ = writer.flush();
}

fn main() {
    println!("Hello, world!");
    let col_map = create_reference_mapping("reference/city-of-london.csv");
    let london_map = create_reference_mapping("reference/london.csv");
    let eng_map = create_reference_mapping("reference/england.csv");

    filter_and_write(
        create_ppd_mapping("estates/barbican_adapted.csv"),
        vec![col_map.clone(), london_map.clone(), eng_map.clone()],
        |x| x < 3,
        |x| x < 7300,
        Some(10),
        "output-final/barbican-output.csv",
    );

    filter_and_write(
        create_ppd_mapping("estates/golden_lane.csv"),
        vec![col_map, london_map, eng_map],
        |x| x < 3,
        |x| x < 7300,
        None,
        "output-final/golden-lane-output.csv",
    );
}
