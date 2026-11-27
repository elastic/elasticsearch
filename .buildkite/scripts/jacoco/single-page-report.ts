/**
 * Turns a JaCoCo XML report into one self-contained, browsable HTML page.
 *
 * JaCoCo's own HTML report for :server is ~15k files. Uploading those as individual Buildkite
 * artifacts took ~20 minutes per CI job, almost all of it per-artifact API overhead. This page
 * holds the same information (package and file tables, per-method counters and line-by-line
 * source coverage) in a single file: the coverage data and the sources are gzipped and embedded
 * as base64, and the browser inflates them with DecompressionStream.
 *
 * Usage:
 *   node single-page-report.ts --xml <jacoco.xml> --out <page.html> [--title <title>] [--src <dir>]...
 *
 * Each --src is a source root (e.g. server/src/main/java); a source file is looked up as
 * <src>/<package path>/<file name>. Files that are not found are listed without source.
 */
import { existsSync, readFileSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

/** Counter types kept from the XML, in the order they are stored in {@link Counters}. */
export const COUNTER_TYPES = ["INSTRUCTION", "BRANCH", "LINE", "METHOD", "CLASS"] as const;

/** `[missed, covered]` pairs, one per entry in {@link COUNTER_TYPES}, flattened. */
export type Counters = number[];

export interface MethodCoverage {
  name: string;
  line: number;
  counters: Counters;
}

export interface SourceFileCoverage {
  name: string;
  counters: Counters;
  /** Flattened `[nr, mi, ci, mb, cb]` tuples, one per line that has bytecode. */
  lines: number[];
  methods: MethodCoverage[];
}

export interface PackageCoverage {
  /** Slash-separated, as in the XML (`org/elasticsearch/common`). Empty for the default package. */
  name: string;
  counters: Counters;
  sourceFiles: SourceFileCoverage[];
}

export interface CoverageReport {
  name: string;
  counters: Counters;
  packages: PackageCoverage[];
}

/** `org/elasticsearch/Foo$Bar` -> `Foo$Bar` */
function lastSegment(path: string): string {
  return path.slice(path.lastIndexOf("/") + 1);
}

function emptyCounters(): Counters {
  return new Array(COUNTER_TYPES.length * 2).fill(0);
}

const XML_ENTITIES: Record<string, string> = { lt: "<", gt: ">", amp: "&", quot: '"', apos: "'" };

function decodeXml(value: string): string {
  return value.replace(/&(lt|gt|amp|quot|apos|#\d+|#x[0-9a-fA-F]+);/g, (_, entity: string) => {
    if (entity.startsWith("#x")) {
      return String.fromCodePoint(parseInt(entity.slice(2), 16));
    }
    if (entity.startsWith("#")) {
      return String.fromCodePoint(parseInt(entity.slice(1), 10));
    }
    return XML_ENTITIES[entity];
  });
}

function parseAttributes(raw: string): Record<string, string> {
  const attrs: Record<string, string> = {};
  for (const match of raw.matchAll(/([\w:-]+)\s*=\s*"([^"]*)"/g)) {
    attrs[match[1]] = decodeXml(match[2]);
  }
  return attrs;
}

const PRIMITIVES: Record<string, string> = {
  B: "byte",
  C: "char",
  D: "double",
  F: "float",
  I: "int",
  J: "long",
  S: "short",
  Z: "boolean",
  V: "void",
};

/** `(Ljava/lang/String;[II)V` -> `String, int[], int` */
export function describeParameters(desc: string): string {
  const params: string[] = [];
  const end = desc.indexOf(")");
  let i = desc.indexOf("(") + 1;
  while (i > 0 && i < end) {
    let dims = 0;
    while (desc[i] === "[") {
      dims++;
      i++;
    }
    let type: string;
    if (desc[i] === "L") {
      const semi = desc.indexOf(";", i);
      type = lastSegment(desc.slice(i + 1, semi)).replace(/\$/g, ".");
      i = semi + 1;
    } else {
      type = PRIMITIVES[desc[i]] ?? desc[i];
      i++;
    }
    params.push(type + "[]".repeat(dims));
  }
  return params.join(", ");
}

/**
 * A readable method name, like JaCoCo's HTML report: constructors use the class name, static
 * initializers are `static {...}`, and members of nested classes are prefixed with that class.
 */
export function methodDisplayName(className: string, name: string, desc: string, fileName: string): string {
  const simpleName = lastSegment(className);
  const fileBase = fileName.replace(/\.java$/, "");
  const prefix = simpleName === fileBase ? "" : simpleName.replace(/\$/g, ".") + ".";
  if (name === "<clinit>") {
    return prefix + "static {...}";
  }
  if (name === "<init>") {
    return simpleName.replace(/\$/g, ".") + "(" + describeParameters(desc) + ")";
  }
  return prefix + name + "(" + describeParameters(desc) + ")";
}

/**
 * Parses JaCoCo's XML report. The format is machine-generated and regular (no CDATA, comments or
 * text content in the elements we read), so a tag tokenizer is enough and keeps this script free of
 * npm dependencies.
 */
export function parseJacocoXml(xml: string): CoverageReport {
  const report: CoverageReport = { name: "", counters: emptyCounters(), packages: [] };
  const stack: string[] = [];

  let pkg: PackageCoverage | null = null;
  let methodsByFile = new Map<string, MethodCoverage[]>();
  let cls: { name: string; sourceFile: string } | null = null;
  let method: (MethodCoverage & { desc: string; rawName: string }) | null = null;
  let sourceFile: SourceFileCoverage | null = null;

  const open = (tag: string, attrs: Record<string, string>) => {
    switch (tag) {
      case "report":
        report.name = attrs.name ?? "";
        break;
      case "package":
        pkg = { name: attrs.name ?? "", counters: emptyCounters(), sourceFiles: [] };
        methodsByFile = new Map();
        break;
      case "class":
        cls = { name: attrs.name ?? "", sourceFile: attrs.sourcefilename ?? "" };
        break;
      case "method":
        method = {
          name: "",
          rawName: attrs.name ?? "",
          desc: attrs.desc ?? "",
          line: Number(attrs.line ?? 0),
          counters: emptyCounters(),
        };
        break;
      case "sourcefile":
        sourceFile = { name: attrs.name ?? "", counters: emptyCounters(), lines: [], methods: [] };
        break;
      case "line":
        sourceFile?.lines.push(
          Number(attrs.nr),
          Number(attrs.mi ?? 0),
          Number(attrs.ci ?? 0),
          Number(attrs.mb ?? 0),
          Number(attrs.cb ?? 0),
        );
        break;
      case "counter": {
        const index = COUNTER_TYPES.findIndex((type) => type === attrs.type);
        if (index < 0) {
          break;
        }
        const target = countersFor(stack[stack.length - 1]);
        if (target) {
          target[index * 2] = Number(attrs.missed ?? 0);
          target[index * 2 + 1] = Number(attrs.covered ?? 0);
        }
        break;
      }
      default:
        // sessioninfo, group and anything newer: not shown on the page
        break;
    }
  };

  const countersFor = (parent: string | undefined): Counters | null => {
    switch (parent) {
      case "method":
        return method?.counters ?? null;
      case "sourcefile":
        return sourceFile?.counters ?? null;
      case "package":
        return pkg?.counters ?? null;
      case "report":
        return report.counters;
      default:
        // class and group totals are derivable from what we keep
        return null;
    }
  };

  const close = (tag: string) => {
    switch (tag) {
      case "method":
        if (method && cls) {
          method.name = methodDisplayName(cls.name, method.rawName, method.desc, cls.sourceFile);
          const list = methodsByFile.get(cls.sourceFile) ?? [];
          list.push({ name: method.name, line: method.line, counters: method.counters });
          methodsByFile.set(cls.sourceFile, list);
        }
        method = null;
        break;
      case "class":
        cls = null;
        break;
      case "sourcefile":
        if (sourceFile && pkg) {
          pkg.sourceFiles.push(sourceFile);
        }
        sourceFile = null;
        break;
      case "package":
        if (pkg) {
          for (const file of pkg.sourceFiles) {
            file.methods = (methodsByFile.get(file.name) ?? []).sort((a, b) => a.line - b.line);
          }
          pkg.sourceFiles.sort((a, b) => a.name.localeCompare(b.name));
          report.packages.push(pkg);
        }
        pkg = null;
        break;
      default:
        break;
    }
  };

  // <?xml ...?> and <!DOCTYPE ...> do not match: the tag name must start with a letter.
  for (const match of xml.matchAll(/<(\/?)([A-Za-z][\w-]*)([^>]*?)(\/?)>/g)) {
    const [, closing, tag, rawAttrs, selfClosing] = match;
    if (closing) {
      stack.pop();
      close(tag);
      continue;
    }
    open(tag, parseAttributes(rawAttrs));
    if (selfClosing) {
      close(tag);
    } else {
      stack.push(tag);
    }
  }

  report.packages.sort((a, b) => a.name.localeCompare(b.name));
  return report;
}

/** Compact JSON shape read by viewer.html. Keep the two in sync. */
export interface ViewerIndex {
  v: 1;
  title: string;
  generated: string;
  counterTypes: readonly string[];
  totals: Counters;
  /** n: package name, c: counters, f: files (n: name, c: counters, s: index into sources or -1, l: lines, m: methods as [name, line, counters]) */
  packages: {
    n: string;
    c: Counters;
    f: { n: string; c: Counters; s: number; l: number[]; m: [string, number, Counters][] }[];
  }[];
}

/**
 * Builds the viewer's index and the list of sources it points into. `readSource` gets the
 * package-relative path (`org/elasticsearch/Foo.java`) and returns null when it has no source.
 */
export function buildViewerData(
  report: CoverageReport,
  title: string,
  generated: Date,
  readSource: (relativePath: string) => string | null,
): { index: ViewerIndex; sources: string[]; missingSources: number } {
  const sources: string[] = [];
  let missingSources = 0;
  const packages = report.packages.map((p) => ({
    n: p.name,
    c: p.counters,
    f: p.sourceFiles.map((f) => {
      const source = readSource(p.name ? `${p.name}/${f.name}` : f.name);
      if (source === null) {
        missingSources++;
      } else {
        sources.push(source);
      }
      return {
        n: f.name,
        c: f.counters,
        s: source === null ? -1 : sources.length - 1,
        l: f.lines,
        m: f.methods.map((m): [string, number, Counters] => [m.name, m.line, m.counters]),
      };
    }),
  }));
  return {
    index: {
      v: 1,
      title,
      generated: generated.toISOString(),
      counterTypes: COUNTER_TYPES,
      totals: report.counters,
      packages,
    },
    sources,
    missingSources,
  };
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

function gzipBase64(value: unknown): string {
  return gzipSync(Buffer.from(JSON.stringify(value), "utf8"), { level: 9 }).toString("base64");
}

/**
 * Fills the viewer template. The data is base64 (no `<`), so it cannot close the script element it
 * sits in, and the title is HTML-escaped; everything else reaches the DOM through textContent.
 */
export function renderHtml(template: string, title: string, index: ViewerIndex, sources: string[]): string {
  const values: Record<string, string> = {
    TITLE: escapeHtml(title),
    INDEX_DATA: gzipBase64(index),
    SOURCES_DATA: gzipBase64(sources),
  };
  return template.replace(/\{\{(TITLE|INDEX_DATA|SOURCES_DATA)\}\}/g, (_, key: string) => values[key]);
}

interface Args {
  xml: string;
  out: string;
  title: string;
  srcDirs: string[];
}

export function parseArgs(argv: string[]): Args {
  const args: Partial<Args> & { srcDirs: string[] } = { srcDirs: [] };
  for (let i = 0; i < argv.length; i++) {
    const value = argv[i + 1];
    if (value === undefined) {
      throw new Error(`missing value for ${argv[i]}`);
    }
    switch (argv[i]) {
      case "--xml":
        args.xml = value;
        break;
      case "--out":
        args.out = value;
        break;
      case "--title":
        args.title = value;
        break;
      case "--src":
        args.srcDirs.push(value);
        break;
      default:
        throw new Error(`unknown argument ${argv[i]}`);
    }
    i++;
  }
  if (!args.xml || !args.out) {
    throw new Error("usage: single-page-report.ts --xml <jacoco.xml> --out <page.html> [--title <title>] [--src <dir>]...");
  }
  return { xml: args.xml, out: args.out, title: args.title ?? "Coverage report", srcDirs: args.srcDirs };
}

function main(argv: string[]): void {
  const started = Date.now();
  const args = parseArgs(argv);
  const report = parseJacocoXml(readFileSync(args.xml, "utf8"));
  const { index, sources, missingSources } = buildViewerData(report, args.title, new Date(), (relativePath) => {
    for (const dir of args.srcDirs) {
      const path = join(dir, relativePath);
      if (existsSync(path)) {
        return readFileSync(path, "utf8");
      }
    }
    return null;
  });
  const template = readFileSync(new URL("./viewer.html", import.meta.url), "utf8");
  writeFileSync(args.out, renderHtml(template, args.title, index, sources));

  const files = index.packages.reduce((n, p) => n + p.f.length, 0);
  const sizeMiB = (statSync(args.out).size / (1024 * 1024)).toFixed(1);
  console.log(
    `Wrote ${args.out}: ${index.packages.length} packages, ${files} source files ` +
      `(${missingSources} without source), ${sizeMiB} MiB, in ${Date.now() - started} ms`,
  );
}

// Compare real paths: the checkout may be reached through a symlink, and a mismatch would skip main() silently.
if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(import.meta.filename)) {
  main(process.argv.slice(2));
}
