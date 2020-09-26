/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.ir.IRNode.IRDecoration;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.util.Objects;

public class IRDecorations {

    public abstract static class IRDType implements IRDecoration {

        private final Class<?> type;

        public IRDType(Class<?> type) {
            this.type = Objects.requireNonNull(type);
        }

        public Class<?> getType() {
            return type;
        }

        public String getCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(type);
        }
    }

    public static class IRDExpressionType extends IRDType {

        public IRDExpressionType(Class<?> expressionType) {
            super(expressionType);
        }
    }
}
