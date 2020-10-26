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

import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.IRNode.IRDecoration;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.util.Objects;

public class IRDecorations {

    public abstract static class IRDType extends IRDecoration<Class<?>> {

        public IRDType(Class<?> type) {
            super(Objects.requireNonNull(type));
        }

        @Override
        public String toString() {
            return PainlessLookupUtility.typeToCanonicalTypeName(getValue());
        }
    }

    public static class IRDExpressionType extends IRDType {

        public IRDExpressionType(Class<?> expressionType) {
            super(expressionType);
        }
    }

    public static class IRDBinaryType extends IRDType {

        public IRDBinaryType(Class<?> binaryType) {
            super(binaryType);
        }
    }

    public static class IRDShiftType extends IRDType {

        public IRDShiftType(Class<?> shiftType) {
            super(shiftType);
        }
    }

    public static class IRDOperation extends IRDecoration<Operation> {

        public IRDOperation(Operation operation) {
            super(Objects.requireNonNull(operation));
        }

        @Override
        public String toString() {
            return getValue().symbol;
        }
    }

    public static class IRDFlags extends IRDecoration<Integer> {

        public IRDFlags(Integer flags) {
            super(Objects.requireNonNull(flags));
        }
    }

    public static class IRDRegexLimit extends IRDecoration<Integer> {

        public IRDRegexLimit(Integer regexLimit) {
            super(Objects.requireNonNull(regexLimit));
        }
    }
}
