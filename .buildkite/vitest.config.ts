import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    root: ".",
    experimental: {
      viteModuleRunner: false,
      nodeLoader: true,
    },
  },
});
