import { transformTaskStatus } from "./smart-retry";
import type { TaskStatusReport } from "./types";

const taskStatusPath = process.argv[2];
const outputPath = process.argv[3];
const testseed = process.argv[4] ?? "";

if (!taskStatusPath || !outputPath) {
  console.error("Usage: bun transform.ts <task-status.json> <output.json> [testseed]");
  process.exit(1);
}

const taskStatusJson = await Bun.file(taskStatusPath).text();
const report: TaskStatusReport = JSON.parse(taskStatusJson);
const result = transformTaskStatus(report, testseed);
await Bun.write(outputPath, JSON.stringify(result, null, 2));
