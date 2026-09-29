/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds partition information detected from file paths.
 * Maps partition column names to their inferred types, and each file path
 * to its extracted partition key-value pairs.
 * Both maps preserve insertion order so that partition columns appear
 * in the same order they are declared in the path.
 */
public record PartitionMetadata(Map<String, DataType> partitionColumns, Map<StoragePath, Map<String, Object>> filePartitionValues) {

    public static final PartitionMetadata EMPTY = new PartitionMetadata(Map.of(), Map.of());

    public PartitionMetadata {
        if (partitionColumns == null) {
            throw new IllegalArgumentException("partitionColumns cannot be null");
        }
        if (filePartitionValues == null) {
            throw new IllegalArgumentException("filePartitionValues cannot be null");
        }
        partitionColumns = unmodifiableOrderedCopy(partitionColumns);
        filePartitionValues = unmodifiableOrderedFileValues(filePartitionValues);
    }

    private static <K, V> Map<K, V> unmodifiableOrderedCopy(Map<K, V> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private static Map<StoragePath, Map<String, Object>> unmodifiableOrderedFileValues(Map<StoragePath, Map<String, Object>> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<StoragePath, Map<String, Object>> copy = Maps.newLinkedHashMapWithExpectedSize(source.size());
        for (Map.Entry<StoragePath, Map<String, Object>> entry : source.entrySet()) {
            copy.put(entry.getKey(), unmodifiableOrderedCopy(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }

    public boolean isEmpty() {
        return partitionColumns.isEmpty();
    }

    /**
     * These partition columns, valued over a different set of files.
     * <p>
     * The columns are the dataset's schema: which they are, and what type each holds, is decided once at
     * resolution, and the plan's output attributes already carry that answer. The values are per file, so they
     * belong to whichever listing named the files being read — and when resolution answered the schema from a
     * prefix of the dataset, that is not the listing resolution held. A file the schema's listing never saw has
     * no entry in it, and a missing entry reads as an absent partition value: the column comes back null for
     * every row of that file, silently.
     * <p>
     * Each value is conformed to the column's declared type. The two listings can type a column differently —
     * a wider set of paths can carry a value the prefix's type cannot hold, a narrower one can look more
     * specific than the prefix did — and the declared type is the one the plan is built on, so it wins. A value
     * the declared type cannot hold has none under it, which is confined to the values that genuinely do not fit
     * rather than falling on every file the schema's listing did not reach.
     */
    public PartitionMetadata valuedOver(@Nullable PartitionMetadata scanned) {
        if (partitionColumns.isEmpty() || scanned == null || scanned.filePartitionValues.isEmpty()) {
            return this;
        }
        LinkedHashMap<StoragePath, Map<String, Object>> valued = Maps.newLinkedHashMapWithExpectedSize(scanned.filePartitionValues.size());
        for (Map.Entry<StoragePath, Map<String, Object>> file : scanned.filePartitionValues.entrySet()) {
            LinkedHashMap<String, Object> conformed = Maps.newLinkedHashMapWithExpectedSize(partitionColumns.size());
            for (Map.Entry<String, DataType> column : partitionColumns.entrySet()) {
                String name = column.getKey();
                conformed.put(name, conform(file.getValue().get(name), column.getValue(), scanned.partitionColumns.get(name)));
            }
            valued.put(file.getKey(), conformed);
        }
        return new PartitionMetadata(partitionColumns, valued);
    }

    /**
     * One value under the type the dataset's schema declares for its column, rather than the type the listing it
     * came from inferred. Same type, or no value: nothing to do.
     * <p>
     * Which medium the conversion goes through is the whole of it. A listing that typed the column as text still
     * holds the path's own token, so casting that text to the declared type asks the same question the detector
     * asked and gets the same answer. A listing that typed it as a number has already parsed the token away, and
     * its text is the number's spelling rather than the path's - {@code 1.0} where the folder said {@code 1} -
     * so casting that text to a narrower numeric type fails on every value rather than on the one that did not
     * fit. A number is therefore converted as a number, exactly, and only a value the declared type cannot hold
     * exactly has none under it.
     * <p>
     * Under a declared {@link DataType#KEYWORD} nothing can be recovered: the spelling is the value, a parsed
     * number cannot say whether the folder read {@code 0} or {@code 00}, so the file has no value for that column.
     */
    private static Object conform(@Nullable Object value, DataType declared, @Nullable DataType detected) {
        if (value == null || declared == detected) {
            return value;
        }
        if (declared == DataType.KEYWORD) {
            return detected == DataType.KEYWORD ? value : null;
        }
        if (value instanceof Number number) {
            return exactlyAs(decoded(number, detected), declared);
        }
        try {
            return HivePartitionDetector.castValue(String.valueOf(value), declared);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * The number a detected value stands for, rather than the number it is stored as.
     * <p>
     * {@link DataType#UNSIGNED_LONG} is the one detected type whose in-memory form is not its value: it is held
     * sign-flip-encoded in a {@code long}, and everything that reads one decodes it first (see
     * {@code ExternalScalarRenderer}). Narrowing the raw {@code long} would put every value in the column out by
     * 2^63 — and silently, because a wrongly-converted value is not null and the partition warning only names the
     * ones that are. Every other numeric type stores its own value.
     */
    private static Number decoded(Number number, @Nullable DataType detected) {
        if (detected == DataType.UNSIGNED_LONG && number instanceof Long encoded) {
            return NumericUtils.unsignedLongAsBigInteger(encoded);
        }
        return number;
    }

    /**
     * The same number under {@code declared}, or no value when that type cannot hold it. A fraction under an
     * integral type and a magnitude outside its range are the two ways a value has none; neither invents one.
     * <p>
     * {@link DataType#DOUBLE} is the exception to "exactly", and deliberately: it takes the nearest value a double
     * holds, because that is what the column's type already means everywhere else. Nulling an integer past 2^53
     * would lose a value the query can represent.
     */
    @Nullable
    private static Object exactlyAs(Number number, DataType declared) {
        if (declared == DataType.DOUBLE) {
            return number.doubleValue();
        }
        BigInteger integral;
        try {
            integral = new BigDecimal(number.toString()).toBigIntegerExact();
        } catch (NumberFormatException | ArithmeticException e) {
            // Not finite, or a fraction no integral type holds.
            return null;
        }
        try {
            if (declared == DataType.INTEGER) {
                return integral.intValueExact();
            }
            if (declared == DataType.LONG) {
                return integral.longValueExact();
            }
            // UNSIGNED_LONG and anything else a number could be: the detector's own coercion decides, from a
            // spelling that is now an integer literal rather than a float's.
            return HivePartitionDetector.castValue(integral.toString(), declared);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * These partition columns with no per-file values.
     * <p>
     * The values are evidence, and {@link #nullablePartitionColumns} reads them as evidence about the whole
     * matched fileset: a column present and non-null in every one of them is reported non-null, and the
     * attribute built from it promises the optimizer that no row has a null there. Over a listing that covers
     * part of a dataset that promise is not the listing's to make. Dropping the values makes it unprovable
     * instead of wrong — every column comes back nullable, which is the conservative answer that method already
     * gives when it has no per-file evidence at all.
     */
    public PartitionMetadata withoutPerFileEvidence() {
        return filePartitionValues.isEmpty() ? this : new PartitionMetadata(partitionColumns, Map.of());
    }

    /**
     * Returns the names of partition columns that cannot be proven non-null across the matched
     * fileset. A column is in this set when at least one file in {@link #filePartitionValues}
     * has {@code null} (or no entry) for it — typically because the file lives under a
     * {@code __HIVE_DEFAULT_PARTITION__} directory decoded by {@link HivePartitionDetector}.
     * When per-file values are absent altogether (e.g. metadata constructed without scanning
     * files), every declared column is returned: no evidence means no non-null guarantee.
     * <p>
     * This is a per-query property: different globs over the same dataset can match
     * different subsets of files and therefore yield different null-bearing column sets.
     * Callers use it to decide per-column {@code Nullability.TRUE}/{@code FALSE} when
     * building attributes; columns absent from this set are provably non-null in the
     * matched fileset.
     */
    public Set<String> nullablePartitionColumns() {
        if (partitionColumns.isEmpty()) {
            return Set.of();
        }
        if (filePartitionValues.isEmpty()) {
            // No per-file evidence — stay conservative: every column may be null.
            return Set.copyOf(partitionColumns.keySet());
        }
        Set<String> nullable = new LinkedHashSet<>();
        for (Map<String, Object> values : filePartitionValues.values()) {
            for (String column : partitionColumns.keySet()) {
                if (nullable.contains(column)) {
                    continue;
                }
                // Map.get returns null for both explicit nulls and absent keys; either case means
                // we have no non-null guarantee for this column.
                if (values.get(column) == null) {
                    nullable.add(column);
                }
            }
            if (nullable.size() == partitionColumns.size()) {
                break;
            }
        }
        return Collections.unmodifiableSet(nullable);
    }
}
