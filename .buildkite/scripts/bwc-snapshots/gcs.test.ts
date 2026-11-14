import { describe, expect, test } from "vitest";

import { gsutilArgs } from "./gcs.ts";

describe("gsutil writes", () => {
  test("write an object only if its generation still matches, and keep it out of HTTP caches", () => {
    expect(gsutilArgs.write("my-bucket", "prefix/9.4.txt", "1234")).toEqual([
      "-h",
      "Cache-Control:no-store",
      "-h",
      "x-goog-if-generation-match:1234",
      "cp",
      "-",
      "gs://my-bucket/prefix/9.4.txt",
    ]);
  });

  test("remove an object only if its generation still matches", () => {
    expect(gsutilArgs.remove("my-bucket", "prefix/pending/9.4.json", "1234")).toEqual([
      "-h",
      "x-goog-if-generation-match:1234",
      "rm",
      "gs://my-bucket/prefix/pending/9.4.json",
    ]);
  });
});
