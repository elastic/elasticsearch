/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Default {@link SplitProvider} for file-based sources.
 * Converts each file in the {@link FileSet} into a {@link FileSplit},
 * applying L1 partition pruning when filter hints and partition metadata are available.
 *
 * <p>When filter hints contain resolved {@link Expression} objects, evaluates them against
 * each file's partition values to prune files that cannot match the filter.
 */
public class FileSplitProvider implements SplitProvider {

    @Override
    public List<ExternalSplit> discoverSplits(SplitDiscoveryContext context) {
        FileSet fileSet = context.fileSet();
        if (fileSet == null || fileSet.isResolved() == false) {
            return List.of();
        }

        PartitionMetadata partitionInfo = context.partitionInfo();
        Map<String, Object> config = context.config();
        List<Expression> filterHints = context.filterHints();
        List<ExternalSplit> splits = new ArrayList<>();

        for (StorageEntry entry : fileSet.files()) {
            StoragePath filePath = entry.path();

            Map<String, Object> partitionValues = Map.of();
            if (partitionInfo != null && partitionInfo.isEmpty() == false) {
                Map<String, Object> filePartitions = partitionInfo.filePartitionValues().get(filePath);
                if (filePartitions != null) {
                    partitionValues = filePartitions;
                }
            }

            if (partitionValues.isEmpty() == false && filterHints.isEmpty() == false) {
                if (matchesPartitionFilters(partitionValues, filterHints) == false) {
                    continue;
                }
            }

            String objectName = filePath.objectName();
            String format = null;
            if (objectName != null) {
                int lastDot = objectName.lastIndexOf('.');
                if (lastDot >= 0 && lastDot < objectName.length() - 1) {
                    format = objectName.substring(lastDot);
                }
            }

            splits.add(new FileSplit("file", filePath, 0, entry.length(), format, config, partitionValues));
        }

        return List.copyOf(splits);
    }

    static boolean matchesPartitionFilters(Map<String, Object> partitionValues, List<Expression> filters) {
        for (Expression filter : filters) {
            Boolean result = evaluateFilter(filter, partitionValues);
            if (result != null && result == false) {
                return false;
            }
        }
        return true;
    }

    static Boolean evaluateFilter(Expression filter, Map<String, Object> partitionValues) {
        return switch (filter) {
            case Equals eq -> evaluateComparison(eq.left(), eq.right(), partitionValues, FileSplitProvider::compareEquals);
            case NotEquals neq -> {
                Boolean result = evaluateComparison(neq.left(), neq.right(), partitionValues, FileSplitProvider::compareEquals);
                yield result != null ? result == false : null;
            }
            case GreaterThanOrEqual gte -> evaluateComparison(gte.left(), gte.right(), partitionValues, (a, b) -> compareValues(a, b) >= 0);
            case GreaterThan gt -> evaluateComparison(gt.left(), gt.right(), partitionValues, (a, b) -> compareValues(a, b) > 0);
            case LessThanOrEqual lte -> evaluateComparison(lte.left(), lte.right(), partitionValues, (a, b) -> compareValues(a, b) <= 0);
            case LessThan lt -> evaluateComparison(lt.left(), lt.right(), partitionValues, (a, b) -> compareValues(a, b) < 0);
            case In in -> {
                String columnName = extractColumnName(in.value());
                if (columnName == null || partitionValues.containsKey(columnName) == false) {
                    yield null;
                }
                Object partitionValue = partitionValues.get(columnName);
                Boolean found = false;
                for (Expression listItem : in.list()) {
                    if (listItem instanceof Literal lit) {
                        if (compareEquals(partitionValue, lit.value())) {
                            found = true;
                            break;
                        }
                    } else {
                        yield null;
                    }
                }
                yield found;
            }
            default -> null;
        };
    }

    private static Boolean evaluateComparison(
        Expression left,
        Expression right,
        Map<String, Object> partitionValues,
        BiFunction<Object, Object, Boolean> comparator
    ) {
        String columnName = extractColumnName(left);
        Object literalValue = extractLiteralValue(right);
        if (columnName != null && literalValue != null && partitionValues.containsKey(columnName)) {
            Object partitionValue = partitionValues.get(columnName);
            return partitionValue != null ? comparator.apply(partitionValue, literalValue) : null;
        }
        columnName = extractColumnName(right);
        literalValue = extractLiteralValue(left);
        if (columnName != null && literalValue != null && partitionValues.containsKey(columnName)) {
            Object partitionValue = partitionValues.get(columnName);
            return partitionValue != null ? comparator.apply(partitionValue, literalValue) : null;
        }
        return null;
    }

    private static String extractColumnName(Expression expr) {
        return switch (expr) {
            case FieldAttribute fa -> fa.name();
            case NamedExpression ne -> ne.name();
            default -> null;
        };
    }

    private static Object extractLiteralValue(Expression expr) {
        return switch (expr) {
            case Literal lit -> lit.value();
            default -> null;
        };
    }

    private static boolean compareEquals(Object a, Object b) {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return na.doubleValue() == nb.doubleValue();
        }
        return a.toString().equals(b.toString());
    }

    private static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot compare null partition values");
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof Comparable<?> && b instanceof Comparable<?>) {
            try {
                @SuppressWarnings("rawtypes")
                Comparable ca = (Comparable) a;
                @SuppressWarnings("unchecked")
                int result = ca.compareTo(b);
                return result;
            } catch (ClassCastException e) {
                return a.toString().compareTo(b.toString());
            }
        }
        return a.toString().compareTo(b.toString());
    }
}
