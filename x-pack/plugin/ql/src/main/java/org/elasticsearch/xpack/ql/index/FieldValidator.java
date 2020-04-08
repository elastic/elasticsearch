/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;

import java.util.List;
import java.util.Map;

interface FieldValidator {
    Map<String, InvalidMappedField> validateField(String fieldName, Map<String, FieldCapabilities> types,
            ImmutableOpenMap<String, List<AliasMetadata>> aliases);
}
