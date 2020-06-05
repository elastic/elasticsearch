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

package org.elasticsearch.action.admin.cluster.stats;

import java.util.Map;
import java.util.function.Consumer;

final class MappingVisitor {

    private MappingVisitor() {}

    static void visitMapping(Map<String, ?> mapping, Consumer<Map<String, ?>> fieldMappingConsumer) {
        Object properties = mapping.get("properties");
        if (properties != null && properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            for (Object v : propertiesAsMap.values()) {
                if (v != null && v instanceof Map) {

                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    fieldMappingConsumer.accept(fieldMapping);
                    visitMapping(fieldMapping, fieldMappingConsumer);

                    // Multi fields
                    Object fieldsO = fieldMapping.get("fields");
                    if (fieldsO != null && fieldsO instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> fields = (Map<String, ?>) fieldsO;
                        for (Object v2 : fields.values()) {
                            if (v2 instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, ?> fieldMapping2 = (Map<String, ?>) v2;
                                fieldMappingConsumer.accept(fieldMapping2);
                            }
                        }
                    }
                }
            }
        }
    }

}
