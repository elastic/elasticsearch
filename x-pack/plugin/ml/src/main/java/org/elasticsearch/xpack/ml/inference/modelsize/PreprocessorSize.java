/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;


public interface PreprocessorSize extends Accountable, NamedXContentObject {
    ParseField FIELD_LENGTH = new ParseField("field_length");
    ParseField FEATURE_NAME_LENGTH = new ParseField("feature_name_length");
    ParseField FIELD_VALUE_LENGTHS = new ParseField("field_value_lengths");

    String getName();
}
