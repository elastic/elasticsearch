#!/usr/bin/env python3
"""
Pareto chart generator for TSDB pipeline benchmark results.

Generates one Pareto chart per dataset showing:
- X: Encode throughput (ratio vs ES819-baseline or raw values/s)
- Y: Decode throughput (ratio vs ES819-baseline or raw values/s)
- Color: bytes/val compression ratio (lower is better)
- Red outline: Pareto-optimal points (non-dominated)

Throughput is measured in values/s (from JMH secondary metric valuesProcessed),
which normalizes across different block sizes. This makes pipelines running at
blockSize=1024 directly comparable to ES819-baseline at blockSize=128.

Two baselines are recognized:
- ES819-baseline:            TSDBDocValuesEncoder (production codec, no pipeline)
- ES819-pipeline-reference:  delta-offset-gcd-bitpack via pipeline framework
  (same algorithm as ES819-baseline, measures pipeline architecture overhead)

Reads JMH JSON output format only.
"""
import argparse
import csv
import json
import glob
import os
import sys
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize


def load_jmh_json(path):
    """Load JMH JSON output file."""
    with open(path, "r") as f:
        return json.load(f)


def parse_json_entries(entries):
    """Parse JMH JSON entries and extract metrics."""
    data = {}
    for e in entries:
        bench = e["benchmark"]
        params = e.get("params", {})
        dataset = params.get("datasetName")
        pipeline = params.get("pipeline", "ES819-baseline")
        if pipeline == "delta-offset-gcd-bitpack":
            pipeline = "ES819-pipeline-reference"
        score = e["primaryMetric"]["score"]
        sec = e.get("secondaryMetrics", {})
        encoded_bytes = sec.get("encodedBytes", {}).get("score")
        values_per_s = sec.get("valuesProcessed", {}).get("score")

        if dataset is None:
            continue

        block_size = params.get("blockSize")

        key = (dataset, pipeline)
        data.setdefault(key, {})
        if block_size is not None:
            data[key]["block_size"] = int(block_size)
        if "Encode" in bench:
            data[key]["enc_ops"] = score
            data[key]["enc_bytes"] = encoded_bytes
            data[key]["enc_vals"] = values_per_s
        elif "Decode" in bench:
            data[key]["dec_ops"] = score
            data[key]["dec_vals"] = values_per_s

    return data


def parse_json_files(directory):
    """Parse all JSON files in directory and extract metrics."""
    data = {}
    json_files = glob.glob(os.path.join(directory, "*.json"))

    if not json_files:
        return {}, 0

    for jf in json_files:
        try:
            entries = load_jmh_json(jf)
            file_data = parse_json_entries(entries)
            data.update(file_data)
        except json.JSONDecodeError as e:
            print(f"  Warning: Could not parse {os.path.basename(jf)}: {e}")
            continue

    return data, len(json_files)


def parse_single_json(path):
    """Parse a single JSON file and extract metrics."""
    try:
        entries = load_jmh_json(path)
        return parse_json_entries(entries)
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not parse {path}: {e}")
        return {}


def pareto_front(points):
    """
    Find Pareto-optimal points where higher x and y are better.
    Returns list of (x, y, label) tuples that are non-dominated.
    """
    front = []
    for i, (x, y, label) in enumerate(points):
        dominated = False
        for j, (x2, y2, _) in enumerate(points):
            if j == i:
                continue
            if (x2 >= x and y2 >= y) and (x2 > x or y2 > y):
                dominated = True
                break
        if not dominated:
            front.append((x, y, label))
    return front


def shorten_label(label):
    """Shorten pipeline names for display."""
    short = label
    short = short.replace("ES819-pipeline-reference", "ES819-pipe")
    short = short.replace("ES819-baseline", "ES819")
    short = short.replace("integer-pipeline", "int-pipe")
    short = short.replace("alp-double-lossless", "alp")
    short = short.replace("alp-double-", "alp-")
    short = short.replace("fpc-lossless", "fpc")
    short = short.replace("gorilla-lossless", "gorilla")
    short = short.replace("chimp-lossless", "chimp")
    short = short.replace("chimp128-", "c128-")
    return short


def write_csv(dataset, rows, outdir):
    """Write dataset results to CSV."""
    csv_path = os.path.join(outdir, f"{dataset}-pareto.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pipeline", "enc_x", "dec_y", "bytes_val"])
        for r in rows:
            writer.writerow([r["pipeline"], r["enc_x"], r["dec_y"], r["bytes_val"]])
    print(f"  Saved: {os.path.basename(csv_path)}")


def plot_dataset(dataset, rows, outdir, opts, block_size=None):
    """Generate Pareto chart for a dataset."""
    xs = [r["enc_x"] for r in rows]
    ys = [r["dec_y"] for r in rows]
    cs = [r["bytes_val"] for r in rows]
    labels = [r["pipeline"] for r in rows]

    # Color scale bounds
    vmin = opts.get("vmin") if opts.get("vmin") is not None else min(cs)
    vmax = opts.get("vmax") if opts.get("vmax") is not None else max(cs)

    # Two-column layout: scatter plot (left) + bytes/val table (right)
    fig = plt.figure(figsize=(14, 8))
    gs = fig.add_gridspec(1, 2, width_ratios=[3, 1], wspace=0.05)
    ax = fig.add_subplot(gs[0])

    # Scatter plot with color = bytes/val (green=good compression, red=bad)
    sc = ax.scatter(xs, ys, c=cs, cmap="RdYlGn_r", s=80, edgecolor="k", zorder=3,
                    vmin=vmin, vmax=vmax)
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("bytes/val (green=better, red=worse)", fontsize=10)

    # Reference lines at ratio = 1.0 (only in ratio mode)
    if not opts.get("no_baseline"):
        ax.axvline(1.0, color="gray", linestyle="--", linewidth=1, alpha=0.7, zorder=1)
        ax.axhline(1.0, color="gray", linestyle="--", linewidth=1, alpha=0.7, zorder=1)

    # Axis labels
    if opts.get("no_baseline"):
        ax.set_xlabel("Encode throughput (values/s)", fontsize=11)
        ax.set_ylabel("Decode throughput (values/s)", fontsize=11)
    else:
        ax.set_xlabel("Encode values/s ratio vs ES819-baseline (higher is better)", fontsize=11)
        ax.set_ylabel("Decode values/s ratio vs ES819-baseline (higher is better)", fontsize=11)

    title = f"Pipeline Performance: {dataset}"
    if block_size is not None:
        title += f" (blockSize={block_size})"
    ax.set_title(title, fontsize=13, fontweight="bold")

    # Find and highlight Pareto front
    front = pareto_front(list(zip(xs, ys, labels)))
    front_names = {p[2] for p in front}
    fx = [p[0] for p in front]
    fy = [p[1] for p in front]
    ax.scatter(fx, fy, facecolors="none", edgecolors="red", s=160, linewidths=2.5,
               label="Pareto front", zorder=4)

    # Build pipeline-to-number mapping (sorted by bytes/val, matching table order)
    sorted_rows = sorted(rows, key=lambda r: r["bytes_val"])
    label_to_num = {r["pipeline"]: i + 1 for i, r in enumerate(sorted_rows)}

    # Annotate all points with numbers
    for i, label in enumerate(labels):
        num = label_to_num[label]
        ax.annotate(str(num), (xs[i], ys[i]), fontsize=7, ha="center",
                    va="bottom", xytext=(0, 5), textcoords="offset points",
                    fontweight="bold")

    # Add quadrant labels (only in ratio mode)
    if not opts.get("no_baseline"):
        ax.text(0.02, 0.98, "Slower encode\nFaster decode", transform=ax.transAxes,
                fontsize=8, alpha=0.5, va="top", ha="left")
        ax.text(0.98, 0.98, "Faster encode\nFaster decode", transform=ax.transAxes,
                fontsize=8, alpha=0.5, va="top", ha="right")
        ax.text(0.02, 0.02, "Slower encode\nSlower decode", transform=ax.transAxes,
                fontsize=8, alpha=0.5, va="bottom", ha="left")
        ax.text(0.98, 0.02, "Faster encode\nSlower decode", transform=ax.transAxes,
                fontsize=8, alpha=0.5, va="bottom", ha="right")

    ax.legend(loc="lower right", fontsize=9)
    ax.grid(True, alpha=0.3, zorder=0)

    # Bytes/val table (right panel), sorted by bytes/val ascending (best first)
    ax_t = fig.add_subplot(gs[1])
    ax_t.axis("off")
    ax_t.set_title("bytes/val (sorted)", fontsize=10, fontweight="bold", pad=12)

    cell_text = []
    cell_colors = []
    cmap = plt.cm.RdYlGn_r
    norm = Normalize(vmin=vmin, vmax=vmax)
    for i, r in enumerate(sorted_rows):
        num = str(i + 1)
        name = shorten_label(r["pipeline"]).replace("\n", " ")
        bv = f"{r['bytes_val']:.2f}"
        cell_text.append([num, name, bv])
        bv_color = cmap(norm(r["bytes_val"]))
        cell_colors.append(["white", "white", bv_color])

    table = ax_t.table(
        cellText=cell_text,
        colLabels=["#", "Pipeline", "B/val"],
        cellColours=cell_colors,
        colColours=["#e6e6e6", "#e6e6e6", "#e6e6e6"],
        loc="upper center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.auto_set_column_width([0, 1, 2])

    # Bold Pareto-optimal rows with star prefix
    for i, r in enumerate(sorted_rows):
        row_idx = i + 1  # +1 for header row
        if r["pipeline"] in front_names:
            table[row_idx, 1].get_text().set_fontweight("bold")
            table[row_idx, 1].get_text().set_text(f"\u2605 {cell_text[i][1]}")

    out = os.path.join(outdir, f"{dataset}-pareto.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {os.path.basename(out)}")
    return out


def process_data(data, outdir, opts):
    """Process extracted data and generate plots."""
    # Group by dataset and compute bytes_val
    datasets = {}
    for (dataset, pipeline), v in data.items():
        if "enc_vals" not in v or "dec_vals" not in v:
            continue
        if v.get("enc_bytes") is None or v.get("enc_vals") is None:
            continue
        if v.get("dec_vals") is None:
            continue
        bytes_val = v["enc_bytes"] / v["enc_vals"]
        block_size = v.get("block_size")
        datasets.setdefault(dataset, []).append((pipeline, v, bytes_val, block_size))

    print(f"Found {len(datasets)} datasets with complete metrics")

    # Compute global vmin/vmax for consistent color scale if not specified
    all_bytes_val = []
    for rows in datasets.values():
        all_bytes_val.extend([bv for (_, _, bv, _) in rows])

    if opts.get("vmin") is None and all_bytes_val:
        opts["vmin"] = min(all_bytes_val)
    if opts.get("vmax") is None and all_bytes_val:
        opts["vmax"] = max(all_bytes_val)

    generated = []
    no_baseline = opts.get("no_baseline", False)

    for dataset in sorted(datasets.keys()):
        rows = datasets[dataset]
        print(f"\nProcessing: {dataset}")

        # Determine block size from pipeline entries (ES819-baseline has no blockSize param)
        block_sizes = {bs for (_, _, _, bs) in rows if bs is not None}
        block_size = block_sizes.pop() if len(block_sizes) == 1 else None

        if no_baseline:
            # Raw values/s mode - no baseline required
            points = []
            for (pipeline, v, bv, _) in rows:
                points.append({
                    "pipeline": pipeline,
                    "enc_x": v["enc_vals"],
                    "dec_y": v["dec_vals"],
                    "bytes_val": bv
                })
        else:
            # Ratio mode - requires baseline
            baseline = None
            for (pipeline, v, bv, _) in rows:
                if pipeline == "ES819-baseline":
                    baseline = v
                    break

            if baseline is None:
                print(f"  Skipping: no ES819-baseline found (use --no-baseline for raw values/s)")
                continue

            bas_enc = baseline["enc_vals"]
            bas_dec = baseline["dec_vals"]

            points = []
            for (pipeline, v, bv, _) in rows:
                enc_ratio = v["enc_vals"] / bas_enc
                dec_ratio = v["dec_vals"] / bas_dec
                points.append({
                    "pipeline": pipeline,
                    "enc_x": enc_ratio,
                    "dec_y": dec_ratio,
                    "bytes_val": bv
                })

        if len(points) < 2:
            print(f"  Skipping: fewer than 2 data points")
            continue

        print(f"  Found {len(points)} pipelines")

        # Write CSV
        write_csv(dataset, points, outdir)

        # Generate plot
        out = plot_dataset(dataset, points, outdir, opts, block_size=block_size)
        generated.append(out)

    return generated


def main():
    parser = argparse.ArgumentParser(
        description="Generate Pareto charts from TSDB pipeline benchmark results (JSON only).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s bench-run-20260203-subset10
      Process all *.json files in the directory

  %(prog)s -f results/sensor-double.json
      Process a single JSON file

  %(prog)s --no-baseline bench-run-20260203
      Plot raw values/s without requiring ES819-baseline

  %(prog)s --vmin 0.5 --vmax 8.0 bench-run-20260203
      Use fixed color scale across all datasets

Chart interpretation:
  - X axis: Encode throughput (ratio or raw values/s)
  - Y axis: Decode throughput (ratio or raw values/s)
  - Color: bytes/val compression (dark = better, yellow = worse)
  - Red circles: Pareto-optimal pipelines (best tradeoffs)

Output:
  - <dataset>-pareto.png: Pareto chart
  - <dataset>-pareto.csv: Data export (pipeline, enc_x, dec_y, bytes_val)
"""
    )

    parser.add_argument(
        "directory",
        nargs="?",
        help="Directory containing benchmark results (*.json files)"
    )

    parser.add_argument(
        "-f", "--file",
        metavar="JSON",
        help="Process a single JMH JSON file instead of a directory"
    )

    parser.add_argument(
        "-o", "--output",
        metavar="DIR",
        help="Output directory for PNG/CSV files (default: same as input)"
    )

    parser.add_argument(
        "--vmin",
        type=float,
        metavar="FLOAT",
        help="Minimum value for color scale (default: auto from data)"
    )

    parser.add_argument(
        "--vmax",
        type=float,
        metavar="FLOAT",
        help="Maximum value for color scale (default: auto from data)"
    )

    parser.add_argument(
        "--no-baseline",
        action="store_true",
        help="Plot raw encode/decode values/s without requiring ES819-baseline"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.directory and not args.file:
        parser.print_help()
        sys.exit(1)

    if args.directory and args.file:
        print("ERROR: Specify either a directory or --file, not both")
        sys.exit(1)

    opts = {
        "vmin": args.vmin,
        "vmax": args.vmax,
        "no_baseline": args.no_baseline,
    }

    # Handle single file mode
    if args.file:
        json_path = args.file
        if not os.path.isabs(json_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            json_path = os.path.join(script_dir, json_path)

        if not os.path.isfile(json_path):
            print(f"ERROR: File not found: {json_path}")
            sys.exit(1)

        outdir = args.output if args.output else os.path.dirname(json_path)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)

        print(f"Processing single file: {json_path}")
        data = parse_single_json(json_path)

        if not data:
            print("ERROR: No benchmark data found in file")
            sys.exit(1)

        generated = process_data(data, outdir, opts)
        print(f"\nDone! Generated {len(generated)} chart(s)")
        return

    # Handle directory mode
    directory = args.directory
    if not os.path.isabs(directory):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        directory = os.path.join(script_dir, directory)

    if not os.path.isdir(directory):
        print(f"ERROR: Directory not found: {directory}")
        sys.exit(1)

    outdir = args.output if args.output else directory
    if not os.path.isdir(outdir):
        os.makedirs(outdir)

    print(f"Processing benchmark results from: {directory}")

    data, json_count = parse_json_files(directory)

    if json_count == 0:
        print("ERROR: No *.json files found in directory")
        sys.exit(1)

    if not data:
        print("ERROR: No benchmark data found in JSON files")
        sys.exit(1)

    print(f"Loaded data from {json_count} JSON file(s)")

    generated = process_data(data, outdir, opts)
    print(f"\nDone! Generated {len(generated)} chart(s)")


if __name__ == "__main__":
    main()
