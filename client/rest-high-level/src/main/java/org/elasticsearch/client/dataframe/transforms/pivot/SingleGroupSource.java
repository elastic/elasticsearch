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

package org.elasticsearch.client.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Locale;
import java.util.Objects;

public abstract class SingleGroupSource implements ToXContentObject {

    protected static final ParseField FIELD = new ParseField("field");

    public enum Type {
        TERMS,
        HISTOGRAM,
        DATE_HISTOGRAM;

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    protected final String field;

    public SingleGroupSource(final String field) {
        this.field = field;
    }

    public abstract Type getType();

    public String getField() {
        return field;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof SingleGroupSource == false) {
            return false;
        }

        final SingleGroupSource that = (SingleGroupSource) other;

        return Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
