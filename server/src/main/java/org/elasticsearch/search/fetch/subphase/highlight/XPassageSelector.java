/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.matchhighlight.OffsetRange;
import org.apache.lucene.search.matchhighlight.Passage;
import org.apache.lucene.search.matchhighlight.PassageAdjuster;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

public class XPassageSelector {
    public static final Comparator<Passage> DEFAULT_SCORER =
        (a, b) -> {
            // Compare the number of highlights first.
            int v;
            v = Integer.compare(a.markers.size(), b.markers.size());
            if (v != 0) {
                return v;
            }

            // Total number of characters covered by the highlights.
            int len1 = 0, len2 = 0;
            for (OffsetRange o : a.markers) {
                len1 += o.length();
            }
            for (OffsetRange o : b.markers) {
                len2 += o.length();
            }
            if (len1 != len2) {
                return Integer.compare(len1, len2);
            }

            return Integer.compare(b.from, a.from);
        };

    private final Comparator<Passage> passageScorer;
    private final PassageAdjuster passageAdjuster;

    public XPassageSelector() {
        this(DEFAULT_SCORER, null);
    }

    public XPassageSelector(Comparator<Passage> passageScorer, PassageAdjuster passageAdjuster) {
        this.passageScorer = passageScorer;
        this.passageAdjuster = passageAdjuster;
    }

    public List<Passage> pickBest(
        CharSequence value,
        List<? extends OffsetRange> markers,
        int maxPassageWindow,
        int maxPassages) {
        return pickBest(
            value, markers, maxPassageWindow, maxPassages, List.of(new OffsetRange(0, value.length())));
    }

    public List<Passage> pickBest(
        CharSequence value,
        List<? extends OffsetRange> markers,
        int maxPassageWindow,
        int maxPassages,
        List<OffsetRange> permittedPassageRanges) {
        assert markers instanceof RandomAccess;
        assert permittedPassageRanges instanceof RandomAccess;
        assert sortedAndNonOverlapping(permittedPassageRanges);

        // Handle odd special cases early.
        if (value.length() == 0 || maxPassageWindow == 0) {
            return Collections.emptyList();
        }

        // Best passages so far.
        PriorityQueue<Passage> pq =
            new PriorityQueue<>(markers.size()) {
                @Override
                protected boolean lessThan(Passage a, Passage b) {
                    return passageScorer.compare(a, b) < 0;
                }
            };

        markers = splitOrTruncateToWindows(markers, maxPassageWindow, permittedPassageRanges);

        // Sort markers by their start offset, shortest first.
        markers.sort(Comparator.<OffsetRange>comparingInt(r -> r.from).thenComparingInt(r -> r.to));

        final int max = markers.size();
        int markerIndex = 0;
        nextRange:
        for (OffsetRange range : permittedPassageRanges) {
            final int rangeTo = Math.min(range.to, value.length());

            // Skip ranges outside of the value window anyway.
            if (range.from >= rangeTo) {
                continue;
            }

            while (markerIndex < max) {
                OffsetRange m = markers.get(markerIndex);

                // Markers are sorted so if the current marker's start is past the range,
                // we can advance, but we need to check the same marker against the new range.
                if (m.from >= rangeTo) {
                    continue nextRange;
                }

                // Check if current marker falls within the range and is smaller than the largest allowed
                // passage window.
                if (m.from >= range.from && m.to <= rangeTo && m.length() <= maxPassageWindow) {

                    // Adjust the window range to center the highlight marker.
                    int from = Math.toIntExact(((long) m.from + m.to - maxPassageWindow) / 2);
                    int to = Math.toIntExact(((long) m.from + m.to + maxPassageWindow) / 2);
                    if (from < range.from) {
                        to += range.from - from;
                        from = range.from;
                    }
                    if (to > rangeTo) {
                        from -= to - rangeTo;
                        to = rangeTo;
                        if (from < range.from) {
                            from = range.from;
                        }
                    }

                    if (from < to && to <= value.length()) {
                        // Find other markers that are completely inside the passage window.
                        ArrayList<OffsetRange> inside = new ArrayList<>();
                        int i = markerIndex;
                        while (i > 0 && markers.get(i - 1).from >= from) {
                            i--;
                        }

                        OffsetRange c;
                        for (; i < max && (c = markers.get(i)).from < to; i++) {
                            if (c.to <= to) {
                                inside.add(c);
                            }
                        }

                        if (!inside.isEmpty()) {
                            pq.insertWithOverflow(new Passage(from, to, inside));
                        }
                    }
                }

                // Advance to the next marker.
                markerIndex++;
            }
        }

        // Collect from the priority queue (reverse the order so that highest-scoring are first).
        Passage[] passages;
        if (pq.size() > 0) {
            passages = new Passage[pq.size()];
            for (int i = pq.size(); --i >= 0; ) {
                passages[i] = pq.pop();
            }
        } else {
            // Handle the default, no highlighting markers case.
            passages = pickDefaultPassage(value, maxPassageWindow, maxPassages, permittedPassageRanges);
        }

        // Correct passage boundaries from maxExclusive window. Typically shrink boundaries until we're
        // on a proper word/sentence boundary.
        if (passageAdjuster != null) {
            passageAdjuster.currentValue(value);
            for (int x = 0; x < passages.length; x++) {
                Passage p = passages[x];
                OffsetRange newRange = passageAdjuster.adjust(p);
                if (newRange.from != p.from || newRange.to != p.to) {
                    assert newRange.from >= p.from && newRange.to <= p.to
                        : "Adjusters must not expand the passage's range: was "
                        + p
                        + " => changed to "
                        + newRange;
                    passages[x] = new Passage(newRange.from, newRange.to, p.markers);
                }
            }
        }

        // Ensure there are no overlaps on passages. In case of conflicts, better score wins.
        int last = 0;
        for (int i = 0; i < passages.length; i++) {
            Passage a = passages[i];
            if (a != null && a.length() > 0) {
                passages[last++] = a;
                for (int j = i + 1; j < passages.length; j++) {
                    Passage b = passages[j];
                    if (b != null) {
                        if (adjecentOrOverlapping(a, b)) {
                            passages[j] = null;
                        }
                    }
                }
            }
        }

        // Remove nullified slots.
        last = Math.min(last, maxPassages);
        if (passages.length != last) {
            passages = ArrayUtil.copyOfSubArray(passages, 0, last);
        }

        // Sort in the offset order again.
        Arrays.sort(passages, Comparator.comparingInt(a -> a.from));

        return Arrays.asList(passages);
    }

    /** Truncate or split highlight markers that cross permitted value boundaries. */
    private List<? extends OffsetRange> splitOrTruncateToWindows(
        List<? extends OffsetRange> markers,
        int maxPassageWindow,
        List<OffsetRange> permittedPassageRanges) {
        // Process markers overlapping with each permitted window.
        ArrayList<OffsetRange> processedMarkers = new ArrayList<>(markers.size());
        for (OffsetRange marker : markers) {
            for (OffsetRange permitted : permittedPassageRanges) {
                boolean newSlice = false;

                int from = marker.from;
                if (from < permitted.from) {
                    from = permitted.from;
                    newSlice = true;
                }

                int to = marker.to;
                if (to > permitted.to) {
                    to = permitted.to;
                    newSlice = true;
                }

                if (from >= to) {
                    continue;
                }

                if (to - from > maxPassageWindow) {
                    to = from + maxPassageWindow;
                    newSlice = true;
                }

                processedMarkers.add(newSlice ? marker.slice(from, to) : marker);
            }
        }
        markers = processedMarkers;
        return markers;
    }

    static boolean sortedAndNonOverlapping(List<? extends OffsetRange> permittedPassageRanges) {
        if (permittedPassageRanges.size() > 1) {
            Iterator<? extends OffsetRange> i = permittedPassageRanges.iterator();
            for (OffsetRange next, previous = i.next(); i.hasNext(); previous = next) {
                next = i.next();
                if (previous.to > next.from) {
                    throw new AssertionError(
                        "Ranges must be sorted and non-overlapping: " + permittedPassageRanges);
                }
            }
        }

        return true;
    }

    /**
     * Invoked when no passages could be selected (due to constraints or lack of highlight markers).
     */
    protected Passage[] pickDefaultPassage(
        CharSequence value,
        int maxCharacterWindow,
        int maxPassages,
        List<OffsetRange> permittedPassageRanges) {
        // Search for the first range that is not empty.
        ArrayList<Passage> defaultPassages = new ArrayList<>();
        for (OffsetRange o : permittedPassageRanges) {
            if (defaultPassages.size() >= maxPassages) {
                break;
            }

            int to = Math.min(value.length(), o.to);
            if (o.from < to) {
                defaultPassages.add(
                    new Passage(
                        o.from,
                        o.from + Math.min(maxCharacterWindow, o.length()),
                        Collections.emptyList()));
            }
        }

        return defaultPassages.toArray(Passage[]::new);
    }

    private static boolean adjecentOrOverlapping(Passage a, Passage b) {
        if (a.from >= b.from) {
            return a.from <= b.to - 1;
        } else {
            return a.to - 1 >= b.from;
        }
    }
}
