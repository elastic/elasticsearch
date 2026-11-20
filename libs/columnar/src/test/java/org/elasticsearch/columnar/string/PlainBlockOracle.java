/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

final class PlainBlockOracle {

    private PlainBlockOracle() {}

    /** The lowest value each entry can hold, from the separators alone. */
    static BytesRef[] mins(PlainSkipFixture fixture) {
        return bounds(fixture.slotValues, fixture.entryFirstSlot, true);
    }

    /** The highest value each entry can hold, from the separators alone. */
    static BytesRef[] maxes(PlainSkipFixture fixture) {
        return bounds(fixture.slotValues, fixture.entryFirstSlot, false);
    }

    /**
     * The lowest or highest value each entry can hold given the slots of an ordered column and the first slot of each
     * entry. The separator of an entry is the first value at or after its first slot, or the last value when there is
     * none, and an entry holds the values from its separator to the separator of the next entry, or to the last value.
     */
    static BytesRef[] bounds(BytesRef[] slotValues, long[] entryFirstSlot, boolean smallest) {
        BytesRef last = null;
        for (BytesRef value : slotValues) {
            if (value != null) {
                last = value;
            }
        }
        final BytesRef[] separators = new BytesRef[entryFirstSlot.length + 1];
        for (int entry = 0; entry < entryFirstSlot.length; entry++) {
            separators[entry] = last;
            for (int address = (int) entryFirstSlot[entry]; address < slotValues.length; address++) {
                if (slotValues[address] != null) {
                    separators[entry] = slotValues[address];
                    break;
                }
            }
        }
        separators[entryFirstSlot.length] = last;
        final boolean ownSeparator = smallest != descending(slotValues);
        final BytesRef[] result = new BytesRef[entryFirstSlot.length];
        for (int entry = 0; entry < result.length; entry++) {
            result[entry] = separators[ownSeparator ? entry : entry + 1];
        }
        return result;
    }

    static boolean descending(BytesRef[] slotValues) {
        BytesRef previous = null;
        for (BytesRef value : slotValues) {
            if (value == null) {
                continue;
            }
            if (previous != null && value.compareTo(previous) != 0) {
                return value.compareTo(previous) < 0;
            }
            previous = value;
        }
        return false;
    }

    /** Whether the closed interval [min, max] holds no value the query asks for; a null interval is a column of nulls. */
    static boolean excludes(BytesRef min, BytesRef max, PlainSkipQuery query) {
        if (min == null) {
            return true;
        }
        if (query.prefix() != null) {
            final BytesRef prefix = query.prefix();
            final boolean allBelow = max.compareTo(prefix) < 0;
            final boolean allAbove = min.compareTo(prefix) > 0 && StringMatchOracle.startsWith(min, prefix) == false;
            return allBelow || allAbove;
        }
        final boolean belowLower = query.lower() != null
            && (max.compareTo(query.lower()) < 0 || (max.compareTo(query.lower()) == 0 && query.includeLower() == false));
        final boolean aboveUpper = query.upper() != null
            && (min.compareTo(query.upper()) > 0 || (min.compareTo(query.upper()) == 0 && query.includeUpper() == false));
        return belowLower || aboveUpper;
    }

    static boolean matches(BytesRef value, PlainSkipQuery query) {
        if (query.prefix() != null) {
            return StringMatchOracle.startsWith(value, query.prefix());
        }
        return StringMatchOracle.inRange(value, query.lower(), query.includeLower(), query.upper(), query.includeUpper());
    }

    /** The documents holding a slot in a block the bounds do not rule out. */
    static List<Integer> keptDocs(PlainSkipFixture fixture, PlainSkipQuery query) {
        final BytesRef[] mins = mins(fixture);
        final BytesRef[] maxes = maxes(fixture);
        final List<Integer> kept = new ArrayList<>();
        int last = -1;
        for (int address = 0; address < fixture.slotValues.length; address++) {
            final int block = fixture.entryOfSlot[address];
            final int doc = fixture.slotDocs[address];
            if (doc != last && excludes(mins[block], maxes[block], query) == false) {
                kept.add(doc);
                last = doc;
            }
        }
        return kept;
    }

    /** The documents holding a value the query matches. */
    static List<Integer> matchingDocs(PlainSkipFixture fixture, PlainSkipQuery query) {
        final List<Integer> matching = new ArrayList<>();
        int last = -1;
        for (int address = 0; address < fixture.slotValues.length; address++) {
            final BytesRef value = fixture.slotValues[address];
            if (value != null && fixture.slotDocs[address] != last && matches(value, query)) {
                last = fixture.slotDocs[address];
                matching.add(last);
            }
        }
        return matching;
    }

    static PlainSkipQuery randomQuery(BytesRef[] pool) {
        return switch (ESTestCase.between(0, 3)) {
            case 0 -> {
                final BytesRef term = randomBound(pool);
                yield PlainSkipQuery.range(term, true, term, true);
            }
            case 1 -> {
                final BytesRef value = randomBound(pool);
                yield PlainSkipQuery.prefix(new BytesRef(value.bytes, value.offset, ESTestCase.between(0, value.length)));
            }
            case 2 -> PlainSkipQuery.range(ESTestCase.rarely() ? null : randomBound(pool), ESTestCase.randomBoolean(), null, true);
            case 3 -> PlainSkipQuery.range(
                ESTestCase.rarely() ? null : randomBound(pool),
                ESTestCase.randomBoolean(),
                ESTestCase.rarely() ? null : randomBound(pool),
                ESTestCase.randomBoolean()
            );
            default -> throw new AssertionError();
        };
    }

    private static BytesRef randomBound(BytesRef[] pool) {
        return PlainSkipTestValues.maybeOffset(randomBoundValue(pool));
    }

    private static BytesRef randomBoundValue(BytesRef[] pool) {
        return switch (ESTestCase.between(0, 3)) {
            case 0, 1 -> ESTestCase.randomFrom(pool);
            case 2 -> PlainSkipTestValues.randomValue();
            case 3 -> {
                final BytesRef base = ESTestCase.randomFrom(pool);
                final byte[] bytes = new byte[base.length + 1];
                System.arraycopy(base.bytes, base.offset, bytes, 0, base.length);
                yield new BytesRef(bytes);
            }
            default -> throw new AssertionError();
        };
    }
}
