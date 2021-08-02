/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSourceTests.randomDateHistogramGroupSource;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSourceTests.randomDateHistogramGroupSourceNoScript;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSourceTests.randomGeoTileGroupSource;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSourceTests.randomHistogramGroupSourceNoScript;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSourceTests.randomTermsGroupSource;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSourceTests.randomTermsGroupSourceNoScript;

public class GroupByOptimizerTests extends ESTestCase {

    public void testOneGroupBy() {
        Map<String, SingleGroupSource> groups = Collections.singletonMap("date1", randomDateHistogramGroupSourceNoScript());

        Collection<Entry<String, SingleGroupSource>> reorderedGroups = GroupByOptimizer.reorderGroups(groups, Collections.emptySet());
        assertEquals(1, reorderedGroups.size());
        Entry<String, SingleGroupSource> entry = reorderedGroups.iterator().next();

        assertEquals("date1", entry.getKey());
        assertEquals(groups.get("date1"), entry.getValue());
    }

    public void testOrderByType() {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        groups.put("terms1", randomTermsGroupSourceNoScript());
        groups.put("date1", randomDateHistogramGroupSourceNoScript());
        groups.put("terms2", randomTermsGroupSourceNoScript());
        groups.put("date2", randomDateHistogramGroupSourceNoScript());
        groups.put("hist1", randomHistogramGroupSourceNoScript());
        groups.put("geo1", randomGeoTileGroupSource());
        groups.put("hist2", randomHistogramGroupSourceNoScript());

        List<String> groupNames = GroupByOptimizer.reorderGroups(Collections.unmodifiableMap(groups), Collections.emptySet())
            .stream()
            .map(e -> e.getKey())
            .collect(Collectors.toList());
        assertEquals(List.of("date1", "date2", "hist1", "hist2", "terms1", "terms2", "geo1"), groupNames);

        // index field preferred over runtime field
        groupNames = GroupByOptimizer.reorderGroups(
            Collections.unmodifiableMap(groups),
            Collections.singleton(groups.get("terms1").getField())
        ).stream().map(e -> e.getKey()).collect(Collectors.toList());
        assertEquals(List.of("date1", "date2", "hist1", "hist2", "terms2", "terms1", "geo1"), groupNames);

        // if both are runtime fields, order as defined in the config
        groupNames = GroupByOptimizer.reorderGroups(
            Collections.unmodifiableMap(groups),
            Set.of(groups.get("terms1").getField(), groups.get("terms2").getField())
        ).stream().map(e -> e.getKey()).collect(Collectors.toList());
        assertEquals(List.of("date1", "date2", "hist1", "hist2", "terms1", "terms2", "geo1"), groupNames);
    }

    public void testOrderByScriptAndType() {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        groups.put("terms1", randomTermsGroupSourceNoScript("t1"));
        // create with scripts
        groups.put("date1", randomDateHistogramGroupSource(Version.CURRENT, true, "d1"));
        groups.put("terms2", randomTermsGroupSource(Version.CURRENT, true, "t2"));
        groups.put("date2", randomDateHistogramGroupSourceNoScript("d2"));
        groups.put("date3", randomDateHistogramGroupSourceNoScript("d3"));

        List<String> groupNames = GroupByOptimizer.reorderGroups(Collections.unmodifiableMap(groups), Collections.emptySet())
            .stream()
            .map(e -> e.getKey())
            .collect(Collectors.toList());
        assertEquals(List.of("date2", "date3", "date1", "terms1", "terms2"), groupNames);

        // prefer no script, runtime field, script
        groupNames = GroupByOptimizer.reorderGroups(
            Collections.unmodifiableMap(groups),
            Collections.singleton(groups.get("date2").getField())
        ).stream().map(e -> e.getKey()).collect(Collectors.toList());
        assertEquals(List.of("date3", "date2", "date1", "terms1", "terms2"), groupNames);
    }

}
