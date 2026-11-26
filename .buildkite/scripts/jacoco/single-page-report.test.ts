import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { describe, expect, test } from "vitest";

import {
  buildViewerData,
  describeParameters,
  methodDisplayName,
  parseArgs,
  parseJacocoXml,
  renderHtml,
} from "./single-page-report.ts";

/** Trimmed-down JaCoCo XML in the shape Gradle's jacocoTestReport writes it (single line, DOCTYPE, no groups). */
const XML =
  '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
  '<!DOCTYPE report PUBLIC "-//JACOCO//DTD Report 1.1//EN" "report.dtd">' +
  '<report name="server">' +
  '<sessioninfo id="host-1" start="1" dump="2"/>' +
  '<package name="org/example">' +
  '<class name="org/example/Foo" sourcefilename="Foo.java">' +
  '<method name="&lt;init&gt;" desc="(Ljava/lang/String;[I)V" line="5">' +
  '<counter type="INSTRUCTION" missed="0" covered="4"/><counter type="LINE" missed="0" covered="2"/>' +
  '<counter type="COMPLEXITY" missed="0" covered="1"/><counter type="METHOD" missed="0" covered="1"/>' +
  "</method>" +
  '<method name="run" desc="(Z)I" line="9">' +
  '<counter type="INSTRUCTION" missed="3" covered="2"/><counter type="BRANCH" missed="1" covered="1"/>' +
  '<counter type="METHOD" missed="0" covered="1"/>' +
  "</method>" +
  '<counter type="INSTRUCTION" missed="3" covered="6"/><counter type="CLASS" missed="0" covered="1"/>' +
  "</class>" +
  '<class name="org/example/Foo$Inner" sourcefilename="Foo.java">' +
  '<method name="&lt;clinit&gt;" desc="()V" line="3">' +
  '<counter type="INSTRUCTION" missed="2" covered="0"/><counter type="METHOD" missed="1" covered="0"/>' +
  "</method>" +
  "</class>" +
  '<sourcefile name="Foo.java">' +
  '<line nr="3" mi="2" ci="0" mb="0" cb="0"/>' +
  '<line nr="5" mi="0" ci="4" mb="0" cb="0"/>' +
  '<line nr="9" mi="3" ci="2" mb="1" cb="1"/>' +
  '<counter type="INSTRUCTION" missed="5" covered="6"/><counter type="BRANCH" missed="1" covered="1"/>' +
  '<counter type="LINE" missed="1" covered="2"/><counter type="METHOD" missed="1" covered="2"/>' +
  '<counter type="CLASS" missed="1" covered="1"/>' +
  "</sourcefile>" +
  '<counter type="INSTRUCTION" missed="5" covered="6"/><counter type="LINE" missed="1" covered="2"/>' +
  "</package>" +
  '<package name="">' +
  '<sourcefile name="Default.java"><line nr="1" mi="1" ci="0" mb="0" cb="0"/>' +
  '<counter type="INSTRUCTION" missed="1" covered="0"/></sourcefile>' +
  "</package>" +
  '<counter type="INSTRUCTION" missed="6" covered="6"/><counter type="COMPLEXITY" missed="9" covered="9"/>' +
  "</report>";

// Counter layout: [missed, covered] for INSTRUCTION, BRANCH, LINE, METHOD, CLASS
describe("parseJacocoXml", () => {
  const report = parseJacocoXml(XML);

  test("reads report totals and ignores counter types it does not keep", () => {
    expect(report.name).toBe("server");
    expect(report.counters).toEqual([6, 6, 0, 0, 0, 0, 0, 0, 0, 0]);
  });

  test("sorts packages and keeps package counters", () => {
    expect(report.packages.map((p) => p.name)).toEqual(["", "org/example"]);
    expect(report.packages[1].counters).toEqual([5, 6, 0, 0, 1, 2, 0, 0, 0, 0]);
  });

  test("reads source file counters and line tuples", () => {
    const foo = report.packages[1].sourceFiles[0];
    expect(foo.name).toBe("Foo.java");
    expect(foo.counters).toEqual([5, 6, 1, 1, 1, 2, 1, 2, 1, 1]);
    expect(foo.lines).toEqual([3, 2, 0, 0, 0, 5, 0, 4, 0, 0, 9, 3, 2, 1, 1]);
  });

  test("attaches methods of all classes in a file, sorted by line, with readable names", () => {
    const foo = report.packages[1].sourceFiles[0];
    expect(foo.methods).toEqual([
      { name: "Foo.Inner.static {...}", line: 3, counters: [2, 0, 0, 0, 0, 0, 1, 0, 0, 0] },
      { name: "Foo(String, int[])", line: 5, counters: [0, 4, 0, 0, 0, 2, 0, 1, 0, 0] },
      { name: "run(boolean)", line: 9, counters: [3, 2, 1, 1, 0, 0, 0, 1, 0, 0] },
    ]);
  });
});

describe("describeParameters", () => {
  test("maps descriptors to simple type names", () => {
    expect(describeParameters("()V")).toBe("");
    expect(describeParameters("(IJZ)V")).toBe("int, long, boolean");
    expect(describeParameters("([[Ljava/util/Map$Entry;D)Ljava/lang/Object;")).toBe("Map.Entry[][], double");
  });
});

describe("methodDisplayName", () => {
  test("uses the class name for constructors and prefixes nested classes", () => {
    expect(methodDisplayName("a/Foo", "<init>", "()V", "Foo.java")).toBe("Foo()");
    expect(methodDisplayName("a/Foo$Bar", "<init>", "(I)V", "Foo.java")).toBe("Foo.Bar(int)");
    expect(methodDisplayName("a/Foo$1", "run", "()V", "Foo.java")).toBe("Foo.1.run()");
    expect(methodDisplayName("a/Foo", "lambda$x$0", "()V", "Foo.java")).toBe("lambda$x$0()");
  });
});

describe("buildViewerData", () => {
  test("indexes sources it finds and marks the rest with -1", () => {
    const report = parseJacocoXml(XML);
    const seen: string[] = [];
    const { index, sources, missingSources } = buildViewerData(report, "t", new Date(0), (path) => {
      seen.push(path);
      return path === "org/example/Foo.java" ? "class Foo {}" : null;
    });
    expect(seen).toEqual(["Default.java", "org/example/Foo.java"]);
    expect(sources).toEqual(["class Foo {}"]);
    expect(missingSources).toBe(1);
    expect(index.packages[0].f[0].s).toBe(-1);
    expect(index.packages[1].f[0].s).toBe(0);
    expect(index.packages[1].f[0].m[1]).toEqual(["Foo(String, int[])", 5, [0, 4, 0, 0, 0, 2, 0, 1, 0, 0]]);
    expect(index.generated).toBe("1970-01-01T00:00:00.000Z");
  });
});

describe("renderHtml", () => {
  const template = readFileSync(new URL("./viewer.html", import.meta.url), "utf8");
  const report = parseJacocoXml(XML);
  const source = "// </script><script>alert(1)</script>\nclass Foo {}";
  const { index, sources } = buildViewerData(report, "x", new Date(0), () => source);
  const html = renderHtml(template, 'part-1 / server <b>"test"</b>', index, sources);

  function embedded(id: string): unknown {
    const match = html.match(new RegExp(`<script type="application/octet-stream" id="${id}">([^<]*)</script>`));
    expect(match).not.toBeNull();
    return JSON.parse(gunzipSync(Buffer.from(match?.[1] ?? "", "base64")).toString("utf8"));
  }

  test("fills every placeholder", () => {
    expect(html).not.toMatch(/\{\{[A-Z_]+\}\}/);
  });

  test("escapes the title", () => {
    expect(html).toContain("<title>part-1 / server &#60;b&#62;&#34;test&#34;&#60;/b&#62;</title>");
    expect(html).not.toContain("<b>");
  });

  test("embeds the index and sources as gzipped base64 that round-trips", () => {
    expect(embedded("index-data")).toEqual(index);
    expect(embedded("sources-data")).toEqual(sources);
    // Source text only exists compressed, so it cannot break out of the page.
    expect(html).not.toContain("alert(1)");
  });
});

describe("parseArgs", () => {
  test("collects repeated --src and defaults the title", () => {
    expect(parseArgs(["--xml", "a.xml", "--out", "o.html", "--src", "s1", "--src", "s2"])).toEqual({
      xml: "a.xml",
      out: "o.html",
      title: "Coverage report",
      srcDirs: ["s1", "s2"],
    });
  });

  test("rejects unknown and incomplete arguments", () => {
    expect(() => parseArgs(["--xml", "a.xml"])).toThrow(/usage/);
    expect(() => parseArgs(["--bogus", "x"])).toThrow(/unknown argument/);
    expect(() => parseArgs(["--xml"])).toThrow(/missing value/);
  });
});
