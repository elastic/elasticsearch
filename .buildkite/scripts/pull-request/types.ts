export type EsPipelineConfig = {
  config?: {
    "allow-labels"?: string | string[];
    "skip-labels"?: string | string[];
    "included-regions"?: string | string[];
    "excluded-regions"?: string | string[];
    "trigger-phrase"?: string;
  };
};

export type BuildkiteStep = {
  steps?: BuildkiteStep[];
  group?: string;
  bwc_template?: boolean;
};

export type BuildkitePipeline = {
  steps?: BuildkiteStep[];
};

export type EsPipeline = EsPipelineConfig &
  BuildkitePipeline & {
    name?: string;
  };
