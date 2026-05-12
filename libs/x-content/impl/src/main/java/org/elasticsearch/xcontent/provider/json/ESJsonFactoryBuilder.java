/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.json.JsonWriteFeature;

public class ESJsonFactoryBuilder extends JsonFactoryBuilder {
    public ESJsonFactoryBuilder() {
        // Write supplementary characters (Java surrogate pairs) as valid 4-byte UTF-8 sequences
        // rather than JSON surrogate escape pairs. Lone surrogates have no valid UTF-8 encoding
        // and will throw at write time, which is the correct behavior.
        enable(JsonWriteFeature.COMBINE_UNICODE_SURROGATES_IN_UTF8);
    }

    @Override
    public JsonFactory build() {
        return new ESJsonFactory(this);
    }
}
