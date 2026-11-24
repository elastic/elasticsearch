/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.equalTo;

public class MergeBranchAlignmentTests extends ESTestCase {

    /**
     * Aligning a merge branch to the merge columns must read the branch output once, not once per merge column. The branch output
     * here counts element reads: indexing it by name up-front reads each attribute exactly once, while scanning it for every merge
     * column (#163560) would read it {@code width * width} times.
     */
    public void testAlignToMergeColumnsReadsBranchOutputOnce() {
        int width = 10_000;
        List<Attribute> attributes = IntStream.range(0, width).mapToObj(i -> (Attribute) referenceAttribute("f" + i, KEYWORD)).toList();
        int[] reads = new int[1];
        List<Attribute> branchOutput = new AbstractList<>() {
            @Override
            public Attribute get(int index) {
                reads[0]++;
                return attributes.get(index);
            }

            @Override
            public int size() {
                return attributes.size();
            }
        };
        // reverse the branch order and add a column the branch does not have
        List<String> mergeColumns = new ArrayList<>(attributes.reversed().stream().map(Attribute::name).toList());
        mergeColumns.add("missing");

        List<Attribute> aligned = MergeBranchAlignment.alignToMergeColumns(mergeColumns, branchOutput);

        assertThat(reads[0], equalTo(width));
        assertThat(aligned, equalTo(attributes.reversed()));
    }
}
