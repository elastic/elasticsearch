export type EsPipelineConfig = {
  config?: {
    "allow-labels"?: string | string[];
    "skip-labels"?: string | string[];
    "included-regions"?: string | string[];
    "excluded-regions"?: string | string[];
    "trigger-phrase"?: string;
    "skip-target-branches"?: string | string[];
    "auto-retry"?: boolean;
  };
};

export type BuildkiteRetryAutomatic = {
  exit_status?: string;
  signal_reason?: string;
  limit: number;
};

export type BuildkiteRetry = {
  automatic?: BuildkiteRetryAutomatic[];
  manual?: {
    allowed?: boolean;
    permit_on_passed?: boolean;
    reason?: string;
  };
};

export type BuildkiteStep = {
  steps?: BuildkiteStep[];
  group?: string;
  bwc_template?: boolean;
  command?: string;
  env?: Record<string, string>;
  retry?: BuildkiteRetry;
  [key: string]: unknown;
};

export type BuildkitePipeline = {
  steps?: BuildkiteStep[];
  env?: Record<string, string>;
};

export type EsPipeline = EsPipelineConfig &
  BuildkitePipeline & {
    name?: string;
  };
