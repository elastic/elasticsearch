/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public abstract class SingleGroupSource implements ToXContentObject {

    protected static final ParseField FIELD = new ParseField("field");
    protected static final ParseField SCRIPT = new ParseField("script");
    protected static final ParseField MISSING_BUCKET = new ParseField("missing_bucket");

    public enum Type {
        TERMS,
        HISTOGRAM,
        DATE_HISTOGRAM,
        GEOTILE_GRID;

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    protected final String field;
    protected final Script script;
    protected final boolean missingBucket;

    public SingleGroupSource(final String field, final Script script, final boolean missingBucket) {
        this.field = field;
        this.script = script;
        this.missingBucket = missingBucket;
    }

    public abstract Type getType();

    public String getField() {
        return field;
    }

    public Script getScript() {
        return script;
    }

    public boolean getMissingBucket() {
        return missingBucket;
    }

    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        if (script != null) {
            builder.field(SCRIPT.getPreferredName(), script);
        }
        if (missingBucket) {
            builder.field(MISSING_BUCKET.getPreferredName(), missingBucket);
        }
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

        return this.missingBucket == that.missingBucket
            && Objects.equals(this.field, that.field)
            && Objects.equals(this.script, that.script);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, script, missingBucket);
    }
}
