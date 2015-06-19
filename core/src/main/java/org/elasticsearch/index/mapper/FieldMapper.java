/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;

/**
 *
 */
public interface FieldMapper extends Mapper {

    String DOC_VALUES_FORMAT = "doc_values_format";

    MappedFieldType fieldType();

    /**
     * List of fields where this field should be copied to
     */
    AbstractFieldMapper.CopyTo copyTo();

    /**
     * Fields might not be available before indexing, for example _all, token_count,...
     * When get is called and these fields are requested, this case needs special treatment.
     *
     * @return If the field is available before indexing or not.
     * */
    boolean isGenerated();

    /**
     * Parse using the provided {@link ParseContext} and return a mapping
     * update if dynamic mappings modified the mappings, or {@code null} if
     * mappings were not modified.
     */
    Mapper parse(ParseContext context) throws IOException;

}
