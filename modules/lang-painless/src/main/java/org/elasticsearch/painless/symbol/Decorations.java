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

import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.Decorator.Condition;
import org.elasticsearch.painless.symbol.Decorator.Decoration;

import java.util.Objects;

public class Decorations {

    // standard input for user statement nodes during semantic phase

    public interface LastSource extends Condition {

    }

    public interface BeginLoop extends Condition {

    }

    public interface InLoop extends Condition {

    }

    public interface LastLoop extends Condition {

    }

    // standard output for user statement nodes during semantic phase

    public interface MethodEscape extends Condition {

    }

    public interface LoopEscape extends Condition {

    }

    public interface AllEscape extends Condition {

    }

    public interface AnyContinue extends Condition {

    }

    public interface AnyBreak extends Condition {

    }

    // standard input for user expression nodes during semantic phase
    
    public interface Read extends Condition {

    }

    public interface Write extends Condition {

    }

    public static class TargetType implements Decoration  {

        private final Class<?> targetType;

        public TargetType(Class<?> targetType) {
            this.targetType = Objects.requireNonNull(targetType);
        }

        public Class<?> getTargetType() {
            return targetType;
        }

        public String getTargetCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(targetType);
        }
    }

    public interface Explicit extends Condition {

    }

    public interface Internal extends Condition {

    }

    // standard output for user expression node during semantic phase
    
    public static class ValueType implements Decoration {

        private final Class<?> valueType;

        public ValueType(Class<?> valueType) {
            this.valueType = Objects.requireNonNull(valueType);
        }

        public Class<?> getValueType() {
            return valueType;
        }

        public String getValueCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(valueType);
        }
    }

    public static class StaticType implements Decoration {

        private final Class<?> staticType;

        public StaticType(Class<?> staticType) {
            this.staticType = Objects.requireNonNull(staticType);
        }

        public Class<?> getStaticType() {
            return staticType;
        }

        public String getStaticCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(staticType);
        }
    }

    public static class PartialCanonicalTypeName implements Decoration {

        private final String partialCanonicalTypeName;

        public PartialCanonicalTypeName(String partialCanonicalTypeName) {
            this.partialCanonicalTypeName = Objects.requireNonNull(partialCanonicalTypeName);
        }

        public String getPartialCanonicalTypeName() {
            return partialCanonicalTypeName;
        }
    }

    public interface DefOptimized extends Condition {

    }
}
