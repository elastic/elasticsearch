/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        try {
            Pattern.compile(arg);
        } catch (PatternSyntaxException e) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_INVALID_ARGUMENT, tc.getTransform(), arg);
            throw new IllegalArgumentException(msg);
        }
    }
}
