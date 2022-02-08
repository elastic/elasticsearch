/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Incremental task to validate a set of YAML files against a schema.
 */
public class ValidateYamlAgainstSchemaTask extends ValidateJsonAgainstSchemaTask {
    @Override
    protected String getFileType() {
        return "YAML";
    }

    protected ObjectMapper getMapper() {
        return new ObjectMapper(new YAMLFactory());
    }
}
