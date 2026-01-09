from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.dataset as ds


def extract_variable_names(**kwargs: bool) -> list[str]:
    """
    Extract variable names.
    """
    # Default values of keyword arguments
    save_csv: bool = False
    simvar_only: bool = False
    lvar_only: bool = False
    for key, value in kwargs.items():
        if key == "save_csv":
            save_csv = value
        elif key == "simvar_only":
            simvar_only = value
        elif key == "lvar_only":
            lvar_only = value
        else:
            raise KeyError("Invalid keyword arguments.")

    if simvar_only and lvar_only:
        raise ValueError("simvar_only and lvar_only cannot be True at the same time.")

    # Import dataset
    dataset = ds.dataset("../data", format="parquet")
    scanner = dataset.scanner(columns=["kind", "name"], batch_size=1_000)

    if save_csv:
        # Output file for variable names
        if simvar_only:
            path_output: str = "../output/var_names_simvar_only.csv"
        elif lvar_only:
            path_output: str = "../output/var_names_lvar_only.csv"
        else:
            path_output: str = "../output/var_names.csv"

        sink = pa.OSFile(path_output, "wb")
        writer = None
        write_options = csv.WriteOptions(include_header=True, quoting_style="none")

    names_seen: set[str] = set()
    for batch in scanner.to_batches():
        if simvar_only:
            mask = pc.equal(batch.column("kind"), "SIMVAR")
            batch = batch.filter(mask)
        elif lvar_only:
            mask = pc.equal(batch.column("kind"), "LVAR")
            batch = batch.filter(mask)

        names = pc.unique(batch.column("name"))

        # Convert to Python only for the small unique set
        new_names: list[str] = [
            name.as_py()
            for name in names
            if name.is_valid and name.as_py() not in names_seen
        ]

        if not new_names:
            continue

        names_seen.update(new_names)

        table = pa.Table.from_arrays(
            [pa.array(new_names, type=pa.string())], names=["name"]
        )

        # if save_csv and writer is None:
        if save_csv and not writer:
            writer = csv.CSVWriter(sink, table.schema, write_options=write_options)

        if save_csv:
            writer.write_table(table)

    if save_csv and writer:
        writer.close()
        sink.close()

    return list(names_seen)


# def analyze_variables(dataset, kind, relevant_names):
#     scanner = dataset.scanner(
#         columns=["ts_us", "name", "value"],
#         filter=((ds.field("kind") == kind) & ds.field("name").isin(relevant_names)),
#     )

#     # Accumulate per variable (keeps it "long", but split by variable)
#     out_ts = defaultdict(list)
#     out_value = defaultdict(list)

#     for batch in scanner.to_batches():
#         ts = batch.column("ts_us")
#         name = batch.column("name")
#         value = batch.column("value")

#         # For each variable, mask rows and append
#         for relevant_name in relevant_names:
#             mask_name = pc.equal(name, relevant_name)
#             ts_rel_name = pc.filter(ts, mask_name)
#             value_rel_name = pc.filter(value, mask_name)
#             # if len(ts_rel_name) > 0:
#             if ts_rel_name:
#                 out_ts[relevant_name].append(ts_rel_name)
#                 out_value[relevant_name].append(value_rel_name)

#     # Concatenate chunks per variable, sort by time, convert to numpy
#     result = {}
#     for relevant_name in relevant_names:
#         if not out_ts[relevant_names]:
#             continue

#         ts_rel_name = ds.dataset


def main() -> None:
    # var_names = extract_variable_names(**{"save_csv": True, "simvar_only": True})
    # print(len(var_names))
    # print(var_names[:10])
    pass


if __name__ == "__main__":
    main()
