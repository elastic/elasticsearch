/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class MigrateAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "migrate";

    public static final ParseField ENABLED_FIELD = new ParseField("enabled");

    private static final ConstructingObjectParser<MigrateAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new MigrateAction(a[0] == null ? true : (boolean) a[0])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    public static MigrateAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final boolean enabled;

    public MigrateAction() {
        this(true);
    }

    public MigrateAction(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        builder.endObject();
        return builder;
    }

    @Override
    public String getName() {
        return NAME;
    }

    boolean isEnabled() {
        return enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(enabled);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return enabled == ((MigrateAction) obj).enabled;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
