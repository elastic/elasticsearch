/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;

public abstract class TextFamilyFieldMapper extends FieldMapper {

    protected TextFamilyFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams params) {
        super(simpleName, mappedFieldType, params);
    }

    public abstract static class Builder extends FieldMapper.Builder {

        private final IndexVersion indexCreatedVersion;
        private final boolean isSyntheticSourceEnabled;
        private final boolean isWithinMultiField;

        protected Builder(String name, IndexVersion indexCreatedVersion, boolean isSyntheticSourceEnabled, boolean isWithinMultiField) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
            this.isWithinMultiField = isWithinMultiField;
        }

        public IndexVersion indexCreatedVersion() {
            return indexCreatedVersion;
        }

        public boolean isSyntheticSourceEnabled() {
            return isSyntheticSourceEnabled;
        }

        public boolean isWithinMultiField() {
            return isWithinMultiField;
        }

    }

}
