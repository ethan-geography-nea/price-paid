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

    fn set_labels(&mut self, v: Vec<String>) {
        self.labels = v;
    }

    fn add_row(&mut self, string: Vec<String>) {
        self.rows.push(string);
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

    fn add_percentage_change(
        &mut self,
        flat_name: String,
        building: String,
        percentages: Vec<(NaiveDate, f32)>,
    ) {
        self.labels.push(format!("{flat_name}, {building}"));
        for row in &mut self.rows {
            row.push("".to_string());
        }

        for (date, pc) in percentages {
            let mut new_row: Vec<String> = vec!["".to_string(); self.labels.len() - 1];
            new_row[0] = date.format("%Y-%m-01").to_string();
            new_row.push(format!("{}", pc));
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

    let mut percentage_change_output = OutputCSV::new();

    for dp in datapoints_to_process.clone() {
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

        // to paint the algorithm:
        // 1. take all the thingys
        // 2. % change to last thingy = x
        // 3. % change of england / london = y
        // 4. % change diff = x - y
        // 5. report % change diff
    }

    println!("{output:?}");

    for ref_map in reference_maps {
        let mut records = vec![];
        for (_, dp) in ref_map.clone() {
            if (min_date.unwrap() <= dp.time) && (dp.time <= max_date.unwrap()) {
                records.push(dp);
            }
        }

        for d in datapoints_to_process.clone() {
            let mut previous_record = None;
            let mut percentages = Vec::new();

            for r in d.records {
                previous_record = match previous_record {
                    None => {
                        percentages.push((r.date, 0f32));
                        Some(r)
                    }
                    Some(prev_r) => {
                        let change: f32 =
                            (r.price_paid - prev_r.price_paid) as f32 / prev_r.price_paid as f32;
                        let new_ref = ref_map[&(r.date.month() as i32, r.date.year() as i32)]
                            .average_price_flats;
                        let original_ref = ref_map
                            [&(prev_r.date.month() as i32, prev_r.date.year() as i32)]
                            .average_price_flats;
                        let change_of_ref: f32 =
                            (new_ref - original_ref) as f32 / original_ref as f32;

                        let percentage_change_diff = change - change_of_ref;
                        percentages.push((r.date, percentage_change_diff * 100f32));
                        Some(prev_r)
                    }
                }
            }

            percentage_change_output.add_percentage_change(
                d.flat,
                format!(
                    "{} v {}",
                    d.building,
                    records.first().unwrap().region.clone()
                ),
                percentages,
            ); // NOT DONE YET; IMPLIMENT WRITING, NEED TO SEE WHAT REFERENCE ITS FROM TOO
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

    let mut writer = csv::Writer::from_path("pc/".to_string() + output_filepath).unwrap();
    _ = writer.write_record(percentage_change_output.labels);
    for row in percentage_change_output.rows {
        let e = writer.write_record(&row);
        println!("{row:?}: {e:?}");
    }

    _ = writer.flush();
}

fn write_all_sale_map(
    col: HashMap<(i32, i32), UKHPIRecord>,
    lon: HashMap<(i32, i32), UKHPIRecord>,
    eng: HashMap<(i32, i32), UKHPIRecord>,
) {
    // algorithm:
    // * get all barbican
    // * add date, barbican, gle, col, lon, eng
    // * add date, price for all

    let mut out = OutputCSV::new();
    let barbican_ppd = create_ppd_mapping("estates/barbican_adapted.csv");
    let gle_ppd = create_ppd_mapping("estates/golden_lane.csv");

    out.set_labels(vec![
        "date".to_string(),
        "barbican".to_string(),
        "golden_lane".to_string(),
        "city_of_london_flats".to_string(),
        "london_flats".to_string(),
        "england_flats".to_string(),
    ]);

    for (_, v) in barbican_ppd {
        for u in v {
            let pp = format!("{}", u.price_paid);
            let date = u.date.format("%Y-%m-%d").to_string();
            out.add_row(vec![
                date,
                pp,
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ]);
        }
    }

    for (_, v) in gle_ppd {
        for u in v {
            let pp = format!("{}", u.price_paid);
            let date = u.date.format("%Y-%m-%d").to_string();
            out.add_row(vec![
                date,
                "".to_string(),
                pp,
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ]);
        }
    }

    let mut col_v: Vec<&UKHPIRecord> = col.values().collect();
    col_v.sort_by(|a, b| a.time.cmp(&b.time));
    for v in col_v {
        let pp = format!("{}", v.average_price_flats);
        let date = v.time.format("%Y-%m-%d").to_string();
        out.add_row(vec![
            date,
            "".to_string(),
            "".to_string(),
            pp,
            "".to_string(),
            "".to_string(),
        ]);
    }

    let mut lon_v: Vec<&UKHPIRecord> = lon.values().collect();
    lon_v.sort_by(|a, b| a.time.cmp(&b.time));
    for v in lon_v {
        let pp = format!("{}", v.average_price_flats);
        let date = v.time.format("%Y-%m-%d").to_string();
        out.add_row(vec![
            date,
            "".to_string(),
            "".to_string(),
            "".to_string(),
            pp,
            "".to_string(),
        ]);
    }

    let mut eng_v: Vec<&UKHPIRecord> = eng.values().collect();
    eng_v.sort_by(|a, b| a.time.cmp(&b.time));
    for v in eng_v {
        let pp = format!("{}", v.average_price_flats);
        let date = v.time.format("%Y-%m-%d").to_string();
        out.add_row(vec![
            date,
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            pp,
        ]);
    }

    println!("{}", out.rows.len());

    let mut writer = csv::Writer::from_path("output/all-prices.csv").unwrap();
    _ = writer.write_record(&out.labels);
    for row in out.rows {
        let e = writer.write_record(&row);
        println!("{:?} {row:?}: {e:?}", out.labels);
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
        "output/barbican-output-4.csv",
    );

    filter_and_write(
        create_ppd_mapping("estates/golden_lane.csv"),
        vec![col_map.clone(), london_map.clone(), eng_map.clone()],
        |x| x < 3,
        |x| x < 7300,
        None,
        "output/golden-lane-output-4.csv",
    );

    write_all_sale_map(col_map, london_map, eng_map);
}
