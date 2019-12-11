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
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents set of {@link LifecycleAction}s which should be executed at a
 * particular point in the lifecycle of an index.
 */
public class Phase implements ToXContentObject {

    static final ParseField MIN_AGE = new ParseField("min_age");
    static final ParseField ACTIONS_FIELD = new ParseField("actions");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Phase, String> PARSER = new ConstructingObjectParser<>("phase", true,
        (a, name) -> new Phase(name, (TimeValue) a[0], ((List<LifecycleAction>) a[1]).stream()
            .collect(Collectors.toMap(LifecycleAction::getName, Function.identity()))));
    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MIN_AGE.getPreferredName()), MIN_AGE, ValueType.VALUE);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(LifecycleAction.class, n, null), v -> {
                throw new IllegalArgumentException("ordered " + ACTIONS_FIELD.getPreferredName() + " are not supported");
            }, ACTIONS_FIELD);
    }

    public static Phase parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final String name;
    private final Map<String, LifecycleAction> actions;
    private final TimeValue minimumAge;

    /**
     * @param name
     *            the name of this {@link Phase}.
     * @param minimumAge
     *            the age of the index when the index should move to this
     *            {@link Phase}.
     * @param actions
     *            a {@link Map} of the {@link LifecycleAction}s to run when
     *            during this {@link Phase}. The keys in this map are the associated
     *            action names.
     */
    public Phase(String name, TimeValue minimumAge, Map<String, LifecycleAction> actions) {
        this.name = name;
        if (minimumAge == null) {
            this.minimumAge = TimeValue.ZERO;
        } else {
            this.minimumAge = minimumAge;
        }
        this.actions = actions;
    }

    /**
     * @return the age of the index when the index should move to this
     *         {@link Phase}.
     */
    public TimeValue getMinimumAge() {
        return minimumAge;
    }

    /**
     * @return the name of this {@link Phase}
     */
    public String getName() {
        return name;
    }

    /**
     * @return a {@link Map} of the {@link LifecycleAction}s to run when during
     *         this {@link Phase}.
     */
    public Map<String, LifecycleAction> getActions() {
        return actions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MIN_AGE.getPreferredName(), minimumAge.getStringRep());
        builder.field(ACTIONS_FIELD.getPreferredName(), actions);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, minimumAge, actions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Phase other = (Phase) obj;
        return Objects.equals(name, other.name) &&
            Objects.equals(minimumAge, other.minimumAge) &&
            Objects.equals(actions, other.actions);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
