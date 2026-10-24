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

    /** The smallest value of each block, null for a block of nulls, worked out from the slots alone. */
    static BytesRef[] mins(PlainSkipFixture fixture) {
        return extremes(fixture, true);
    }

    /** The largest value of each block, null for a block of nulls. */
    static BytesRef[] maxes(PlainSkipFixture fixture) {
        return extremes(fixture, false);
    }

    private static BytesRef[] extremes(PlainSkipFixture fixture, boolean smallest) {
        final BytesRef[] result = new BytesRef[fixture.entryCount()];
        for (int address = 0; address < fixture.slotValues.length; address++) {
            final BytesRef value = fixture.slotValues[address];
            final int block = fixture.entryOfSlot[address];
            if (value == null) {
                continue;
            }
            if (result[block] == null || (smallest ? value.compareTo(result[block]) < 0 : value.compareTo(result[block]) > 0)) {
                result[block] = value;
            }
        }
        return result;
    }

    /** Whether the closed interval [min, max] holds no value the query asks for; a null interval is a block of nulls. */
    static boolean excludes(BytesRef min, BytesRef max, PlainSkipBounds.Query query) {
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

    static boolean matches(BytesRef value, PlainSkipBounds.Query query) {
        if (query.prefix() != null) {
            return StringMatchOracle.startsWith(value, query.prefix());
        }
        return StringMatchOracle.inRange(value, query.lower(), query.includeLower(), query.upper(), query.includeUpper());
    }

    /** The documents holding a slot in a block the bounds do not rule out. */
    static List<Integer> keptDocs(PlainSkipFixture fixture, PlainSkipBounds.Query query) {
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
    static List<Integer> matchingDocs(PlainSkipFixture fixture, PlainSkipBounds.Query query) {
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

    static PlainSkipBounds.Query randomQuery(BytesRef[] pool) {
        return switch (ESTestCase.between(0, 3)) {
            case 0 -> {
                final BytesRef term = randomBound(pool);
                yield PlainSkipBounds.Query.range(term, true, term, true);
            }
            case 1 -> {
                final BytesRef value = randomBound(pool);
                yield PlainSkipBounds.Query.prefix(new BytesRef(value.bytes, value.offset, ESTestCase.between(0, value.length)));
            }
            case 2 -> PlainSkipBounds.Query.range(ESTestCase.rarely() ? null : randomBound(pool), ESTestCase.randomBoolean(), null, true);
            case 3 -> PlainSkipBounds.Query.range(
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
