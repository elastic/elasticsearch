/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;

/**
 * The required and actual header of a PromQL translation attempt. A header contains regular {@link Column}s and
 * load-time {@link EphemeralColumn}s; its {@link Header#groupBy()} identifies the translated subtree's series.
 */
public final class PromqlAttributesTranslationContext {

    private PromqlAttributesTranslationContext() {}

    /** A column exposed by a translated subtree. */
    public sealed interface HeaderColumn permits Column, EphemeralColumn {
        Attribute attribute();
    }

    /** A regular mapped or computed column. */
    public record Column(Attribute attribute) implements HeaderColumn {}

    /** A load-time column containing every series dimension except {@link #exclusions()}. */
    public record EphemeralColumn(Attribute attribute, List<Attribute> exclusions) implements HeaderColumn {
        public EphemeralColumn {
            exclusions = canonicalize(exclusions);
        }

        public static EphemeralColumn of(List<Attribute> exclusions) {
            return new EphemeralColumn(FieldAttribute.timeSeriesAttribute(Source.EMPTY), exclusions);
        }
    }

    /**
     * An immutable top-down requirement. Translation may widen it and retry a child; the child returns a
     * {@link Header} describing the symbolic surface it produced.
     */
    public static final class RequireHeader {
        private final Header requested;
        private final List<EphemeralColumn> strict;

        private RequireHeader(Header requested, List<EphemeralColumn> strict) {
            this.requested = requested;
            this.strict = List.copyOf(strict);
        }

        public static RequireHeader copyInput() {
            EphemeralColumn identity = EphemeralColumn.of(List.of());
            return new RequireHeader(new Header(List.of(identity), List.of(identity)), List.of());
        }

        public static RequireHeader undefined() {
            return new RequireHeader(Header.undefined(), List.of());
        }

        public RequireHeader limitedTo(List<Attribute> labels) {
            List<HeaderColumn> concrete = canonicalize(labels).stream().map(Column::new).map(HeaderColumn.class::cast).toList();
            return new RequireHeader(new Header(concrete, concrete), List.of());
        }

        public RequireHeader including(List<Attribute> labels) {
            var exposed = new ArrayList<>(requested.columns());
            canonicalize(labels).stream().map(Column::new).forEach(exposed::add);
            return new RequireHeader(new Header(requested.groupBy(), exposed), strict);
        }

        public RequireHeader requiring(EphemeralColumn required) {
            boolean alreadyStrict = strict.stream().anyMatch(column -> sameColumn(column, required));
            if (alreadyStrict) {
                return this;
            }
            var requirements = new ArrayList<>(strict);
            requirements.add(required);
            if (requested.ephemeral(required.exclusions()) != null) {
                return new RequireHeader(requested, requirements);
            }
            var exposed = new ArrayList<>(requested.columns());
            exposed.add(required);
            return new RequireHeader(new Header(requested.groupBy(), exposed), requirements);
        }

        public RequireHeader requiring(Header grouping) {
            EphemeralColumn ephemeral = grouping.groupByEphemeral();
            return ephemeral == null ? this : requiring(EphemeralColumn.of(ephemeral.exclusions()));
        }

        public RequireHeader forChild(AcrossSeriesAggregate.Grouping grouping, List<Attribute> labels) {
            return switch (grouping) {
                case BY -> copyInput().including(labels);
                case WITHOUT -> labels.isEmpty() && demandedLabels().isEmpty() == false ? limitedTo(demandedLabels()) : this;
                case NONE -> copyInput();
            };
        }

        public List<Attribute> demandedLabels() {
            return requested.columns().stream().filter(Column.class::isInstance).map(HeaderColumn::attribute).toList();
        }

        /** Whether a returned header exposes every identity projection explicitly required for a retry. */
        public boolean check(Header header) {
            for (EphemeralColumn column : strict) {
                if (header.ephemeral(column.exclusions()) == null) {
                    return false;
                }
            }
            return true;
        }

        Header header() {
            return requested;
        }
    }

    /**
     * The symbolic columns exposed by a translated subtree. Attributes become concrete when analyzer resolution makes
     * them available in the plan output and {@link #bind(List)} replaces their proxies.
     */
    public record Header(List<HeaderColumn> groupBy, List<HeaderColumn> columns) {
        public Header {
            groupBy = distinct(groupBy);
            columns = distinct(columns);
        }

        /** No series identity. */
        public static Header undefined() {
            return new Header(List.of(), List.of());
        }

        /** A concrete header built from known columns. */
        public static Header of(List<Attribute> attributes) {
            List<HeaderColumn> columns = attributes.stream()
                .filter(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()) == false)
                .map(Column::new)
                .map(HeaderColumn.class::cast)
                .toList();
            Attribute timeSeries = findByFieldName(attributes, MetadataAttribute.TIMESERIES);
            if (timeSeries == null) {
                return new Header(columns, columns);
            }
            var result = new ArrayList<HeaderColumn>(columns.size() + 1);
            EphemeralColumn identity = new EphemeralColumn(timeSeries, List.of());
            result.add(identity);
            result.addAll(columns);
            return new Header(List.of(identity), result);
        }

        /** Add regular column requirements without changing the primary grouping. */
        public Header including(List<Attribute> labels) {
            var exposed = new ArrayList<>(columns);
            canonicalize(labels).stream().map(Column::new).forEach(exposed::add);
            return new Header(groupBy, exposed);
        }

        /** Grouping produced by applying an across-series aggregation to this child header. */
        public Header grouped(AcrossSeriesAggregate.Grouping grouping, List<Attribute> labels, List<Attribute> output) {
            return switch (grouping) {
                case BY -> groupedBy(output);
                case WITHOUT -> without(labels);
                case NONE -> undefined();
            };
        }

        /** Header produced by {@code BY(labels)}. Missing labels remain proxies and are null-filled during emission. */
        public Header groupedBy(List<Attribute> labels) {
            List<HeaderColumn> concrete = canonicalize(labels).stream().map(Column::new).map(HeaderColumn.class::cast).toList();
            return new Header(concrete, concrete);
        }

        /** Header produced by {@code WITHOUT(labels)}. */
        public Header without(List<Attribute> labels) {
            List<Attribute> removed = canonicalize(labels);
            EphemeralColumn ephemeral = groupByEphemeral();
            if (ephemeral != null) {
                EphemeralColumn desired = EphemeralColumn.of(unionAttributes(ephemeral.exclusions(), removed));
                EphemeralColumn exposed = ephemeral(desired.exclusions());
                desired = exposed == null ? desired : exposed;
                return new Header(List.of(desired), List.of(desired));
            }
            Set<String> removedNames = fieldNames(removed);
            List<HeaderColumn> concrete = groupBy.stream()
                .filter(Column.class::isInstance)
                .filter(column -> removedNames.contains(canonicalName(column.attribute())) == false)
                .toList();
            return new Header(concrete, concrete);
        }

        /** Regroup this child and preserve non-grouping columns that an ancestor requires and this child exposes. */
        public Header regrouped(Header grouping, Header required) {
            var exposed = new ArrayList<>(grouping.columns);
            for (HeaderColumn demand : required.columns()) {
                if (contains(required.groupBy(), demand)) {
                    continue;
                }
                HeaderColumn actual = columns.stream().filter(column -> sameColumn(column, demand)).findFirst().orElse(null);
                if (actual != null && grouping.canCarry(actual) && contains(exposed, actual) == false) {
                    exposed.add(actual);
                }
            }
            return new Header(grouping.groupBy, exposed);
        }

        private boolean canCarry(HeaderColumn column) {
            EphemeralColumn identity = groupByEphemeral();
            if (column instanceof Column) {
                return identity != null
                    ? fieldNames(identity.exclusions()).contains(canonicalName(column.attribute())) == false
                    : contains(groupBy, column);
            }
            if (column instanceof EphemeralColumn ephemeral) {
                return identity != null && fieldNames(ephemeral.exclusions()).containsAll(fieldNames(identity.exclusions()));
            }
            return false;
        }

        /** Combine exposed columns. An ephemeral primary identity takes precedence over concrete grouping columns. */
        public Header union(Header other) {
            var exposed = new ArrayList<>(columns);
            exposed.addAll(other.columns);

            var grouping = new ArrayList<>(groupBy);
            grouping.addAll(other.groupBy);
            var ephemeral = new ArrayList<EphemeralColumn>();
            for (HeaderColumn column : grouping) {
                if (column instanceof EphemeralColumn candidate
                    && ephemeral.stream().noneMatch(existing -> sameColumn(existing, candidate))) {
                    ephemeral.add(candidate);
                }
            }
            if (ephemeral.size() > 1) {
                throw new IllegalArgumentException("cannot union headers with different ephemeral groupings");
            }
            return new Header(ephemeral.isEmpty() ? grouping : List.of(ephemeral.getFirst()), exposed);
        }

        /** Bind proxy attributes to the corresponding output attributes of a translated plan. */
        public Header bind(List<Attribute> available) {
            return new Header(bind(groupBy, available), bind(columns, available));
        }

        public boolean isEphemeral() {
            return groupByEphemeral() != null;
        }

        public EphemeralColumn groupByEphemeral() {
            return groupBy.size() == 1 && groupBy.getFirst() instanceof EphemeralColumn ephemeral ? ephemeral : null;
        }

        public EphemeralColumn ephemeral(List<Attribute> exclusions) {
            for (HeaderColumn column : columns) {
                if (column instanceof EphemeralColumn ephemeral && sameSet(ephemeral.exclusions(), exclusions)) {
                    return ephemeral;
                }
            }
            return null;
        }

        public List<EphemeralColumn> ephemeralColumns() {
            return columns.stream().filter(EphemeralColumn.class::isInstance).map(EphemeralColumn.class::cast).toList();
        }

        public List<EphemeralColumn> additionalEphemeralColumns() {
            EphemeralColumn primary = groupByEphemeral();
            return ephemeralColumns().stream().filter(column -> primary == null || sameColumn(primary, column) == false).toList();
        }

        public Attribute column(String name) {
            for (HeaderColumn column : columns) {
                if (canonicalName(column.attribute()).equals(name)) {
                    return column.attribute();
                }
            }
            return null;
        }

        public List<Attribute> declared() {
            return groupBy.stream().filter(Column.class::isInstance).map(HeaderColumn::attribute).toList();
        }

        public boolean hasDeclared() {
            return declared().isEmpty() == false;
        }

        public boolean isGrouping(HeaderColumn column) {
            return contains(groupBy, column);
        }

        private static List<HeaderColumn> bind(List<HeaderColumn> columns, List<Attribute> available) {
            var bound = new ArrayList<HeaderColumn>(columns.size());
            for (HeaderColumn column : columns) {
                Attribute attribute = findAvailable(column.attribute(), available);
                if (attribute == null && column instanceof Column) {
                    attribute = findByFieldName(available, canonicalName(column.attribute()));
                }
                if (attribute == null) {
                    bound.add(column);
                } else if (column instanceof EphemeralColumn ephemeral) {
                    bound.add(new EphemeralColumn(attribute, ephemeral.exclusions()));
                } else {
                    bound.add(new Column(attribute));
                }
            }
            return bound;
        }
    }

    private static Attribute findAvailable(Attribute attribute, List<Attribute> available) {
        return available.stream().filter(candidate -> candidate.id().equals(attribute.id())).findFirst().orElse(null);
    }

    private static boolean contains(List<? extends HeaderColumn> columns, HeaderColumn candidate) {
        return columns.stream().anyMatch(column -> sameColumn(column, candidate));
    }

    private static boolean sameColumn(HeaderColumn left, HeaderColumn right) {
        if (left instanceof EphemeralColumn l && right instanceof EphemeralColumn r) {
            return sameSet(l.exclusions(), r.exclusions());
        }
        return left instanceof Column
            && right instanceof Column
            && canonicalName(left.attribute()).equals(canonicalName(right.attribute()));
    }

    private static List<HeaderColumn> distinct(List<? extends HeaderColumn> columns) {
        var result = new ArrayList<HeaderColumn>();
        for (HeaderColumn column : columns) {
            if (contains(result, column) == false) {
                result.add(column);
            }
        }
        return List.copyOf(result);
    }

    private static List<Attribute> canonicalize(List<Attribute> labels) {
        var result = new ArrayList<Attribute>();
        var seen = new LinkedHashSet<String>();
        for (Attribute attribute : labels) {
            if (seen.add(canonicalName(attribute))) {
                result.add(attribute);
            }
        }
        return List.copyOf(result);
    }

    private static List<Attribute> unionAttributes(List<Attribute> left, List<Attribute> right) {
        var result = new ArrayList<Attribute>(left.size() + right.size());
        result.addAll(left);
        result.addAll(right);
        return canonicalize(result);
    }

    private static Set<String> fieldNames(List<Attribute> attributes) {
        var names = new LinkedHashSet<String>();
        attributes.forEach(attribute -> names.add(canonicalName(attribute)));
        return names;
    }

    private static boolean sameSet(List<Attribute> left, List<Attribute> right) {
        return fieldNames(left).equals(fieldNames(right));
    }

    static String canonicalName(Attribute attribute) {
        String name = attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    static Attribute findByFieldName(List<Attribute> attributes, String name) {
        for (Attribute attribute : attributes) {
            if (canonicalName(attribute).equals(name)) {
                return attribute;
            }
        }
        return null;
    }

    public static Attribute findByLabelName(List<Attribute> attributes, String labelName) {
        Attribute bareMatch = null;
        for (Attribute attribute : attributes) {
            if (canonicalName(attribute).equals(labelName)) {
                if (attribute.name().equals(labelName) == false) {
                    return attribute;
                }
                bareMatch = attribute;
            }
        }
        return bareMatch;
    }

    public static List<Attribute> concreteDimensions(List<Attribute> attributes) {
        return attributes.stream()
            .filter(attribute -> attribute instanceof FieldAttribute field && field.isDimension())
            .filter(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()) == false)
            .toList();
    }
}
