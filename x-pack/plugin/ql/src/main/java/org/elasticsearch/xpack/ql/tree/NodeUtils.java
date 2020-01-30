/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.tree;

public abstract class NodeUtils {
    public static <A extends Node<A>, B extends Node<B>> String diffString(A left, B right) {
        return diffString(left.toString(), right.toString());
    }

    public static String diffString(String left, String right) {
        // break the strings into lines
        // then compare each line
        String[] leftSplit = left.split("\\n");
        String[] rightSplit = right.split("\\n");

        // find max - we could use streams but autoboxing is not cool
        int leftMaxPadding = 0;
        for (String string : leftSplit) {
            leftMaxPadding = Math.max(string.length(), leftMaxPadding);
        }

        // try to allocate the buffer - 5 represents the column comparison chars
        StringBuilder sb = new StringBuilder(left.length() + right.length() + Math.max(left.length(),  right.length()) * 3);

        boolean leftAvailable = true, rightAvailable = true;
        for (int leftIndex = 0, rightIndex = 0; leftAvailable || rightAvailable; leftIndex++, rightIndex++) {
            String leftRow = "", rightRow = leftRow;
            if (leftIndex < leftSplit.length) {
                leftRow = leftSplit[leftIndex];
            }
            else {
                leftAvailable = false;
            }
            sb.append(leftRow);
            for (int i = leftRow.length(); i < leftMaxPadding; i++) {
                sb.append(" ");
            }
            // right side still available
            if (rightIndex < rightSplit.length) {
                rightRow = rightSplit[rightIndex];
            }
            else {
                rightAvailable = false;
            }
            if (leftAvailable || rightAvailable) {
                sb.append(leftRow.equals(rightRow) ? " = " : " ! ");
                sb.append(rightRow);
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
