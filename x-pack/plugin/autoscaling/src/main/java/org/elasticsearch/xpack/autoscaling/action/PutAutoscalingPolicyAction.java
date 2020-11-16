/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PutAutoscalingPolicyAction extends ActionType<AcknowledgedResponse> {

    public static final PutAutoscalingPolicyAction INSTANCE = new PutAutoscalingPolicyAction();
    public static final String NAME = "cluster:admin/autoscaling/put_autoscaling_policy";

    private PutAutoscalingPolicyAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, String> PARSER;

        static {
            PARSER = new ConstructingObjectParser<>("put_autocaling_policy_request", false, (c, name) -> {
                @SuppressWarnings("unchecked")
                final List<String> roles = (List<String>) c[0];
                @SuppressWarnings("unchecked")
                final var deciders = (List<Map.Entry<String, AutoscalingDeciderConfiguration>>) c[1];
                return new Request(
                    name,
                    roles != null ? roles.stream().collect(Sets.toUnmodifiableSortedSet()) : null,
                    deciders != null
                        ? new TreeMap<>(deciders.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                        : null
                );
            });
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), AutoscalingPolicy.ROLES_FIELD);
            PARSER.declareNamedObjects(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c, n) -> new AbstractMap.SimpleEntry<>(n, p.namedObject(AutoscalingDeciderConfiguration.class, n, null)),
                AutoscalingPolicy.DECIDERS_FIELD
            );
        }

        private final String name;
        private final SortedSet<String> roles;
        private final SortedMap<String, AutoscalingDeciderConfiguration> deciders;

        public static Request parse(final XContentParser parser, final String name) {
            return PARSER.apply(parser, name);
        }

        public Request(
            final String name,
            final SortedSet<String> roles,
            final SortedMap<String, AutoscalingDeciderConfiguration> deciders
        ) {
            this.name = name;
            this.roles = roles;
            this.deciders = deciders;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            if (in.readBoolean()) {
                roles = in.readSet(StreamInput::readString).stream().collect(Sets.toUnmodifiableSortedSet());
            } else {
                roles = null;
            }
            if (in.readBoolean()) {
                deciders = new TreeMap<>(
                    in.readNamedWriteableList(AutoscalingDeciderConfiguration.class)
                        .stream()
                        .collect(Collectors.toMap(AutoscalingDeciderConfiguration::name, Function.identity()))
                );
            } else {
                deciders = null;
            }
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            if (roles != null) {
                out.writeBoolean(true);
                out.writeCollection(roles, StreamOutput::writeString);
            } else {
                out.writeBoolean(false);
            }
            if (deciders != null) {
                out.writeBoolean(true);
                out.writeNamedWriteableList(deciders.values().stream().collect(Collectors.toUnmodifiableList()));
            } else {
                out.writeBoolean(false);
            }
        }

        public String name() {
            return name;
        }

        public SortedSet<String> roles() {
            return roles;
        }

        public SortedMap<String, AutoscalingDeciderConfiguration> deciders() {
            return deciders;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException exception = null;
            if (roles != null) {
                List<String> errors = roles.stream()
                    .filter(Predicate.not(DiscoveryNode.getPossibleRoleNames()::contains))
                    .collect(Collectors.toList());
                if (errors.isEmpty() == false) {
                    exception = new ActionRequestValidationException();
                    exception.addValidationErrors(errors);
                }
            }

            if (Strings.validFileName(name) == false) {
                exception = ValidateActions.addValidationError(
                    "name must not contain the following characters " + Strings.INVALID_FILENAME_CHARS,
                    exception
                );
            }

            return exception;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) && Objects.equals(roles, request.roles) && Objects.equals(deciders, request.deciders);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, roles, deciders);
        }
    }

}
