/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        new RegexPatternVerifier().verify(arg, tc);

        Pattern pattern = Pattern.compile(arg);
        int groupCount = pattern.matcher("").groupCount();
        List<String> outputs = tc.getOutputs();
        int outputCount = outputs == null ? 0 : outputs.size();
        if (groupCount != outputCount) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_EXTRACT_GROUPS_SHOULD_MATCH_OUTPUT_COUNT,
                    tc.getTransform(), outputCount, arg, groupCount);
            throw new IllegalArgumentException(msg);
        }
    }
}
