/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.datastreams.logsdb.qa.e.FieldValeMatcherException;
import org.elasticsearch.datastreams.logsdb.qa.ex.FieldNameMatcherException;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MatcherException;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MissingFieldMatcherException;
import org.elasticsearch.datastreams.logsdb.qa.matchers.Matcher;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

public class SyntheticSourceSearchHitMatcher extends Matcher<SearchHit> {
    @Override
    public void match(final SearchHit a, final SearchHit b) throws MatcherException {
        final Map<String, DocumentField> aFields = a.getDocumentFields();
        final Map<String, DocumentField> bFields = b.getDocumentFields();
        for (var aEntry: aFields.entrySet()) {
            compare(aEntry, bFields);
        }
        for (var bEntry: bFields.entrySet()) {
            compare(bEntry, aFields);
        }
    }

    private static void compare(Map.Entry<String, DocumentField> aEntry, Map<String, DocumentField> bFields) throws MissingFieldMatcherException, FieldNameMatcherException, FieldValeMatcherException {
        final DocumentField bEntry = bFields.get(aEntry.getKey());
        if (bEntry == null) {
            throw new MissingFieldMatcherException(aEntry.getKey());
        }
        final DocumentField aField = aEntry.getValue();
        final DocumentField bField = bEntry.getValue();

        if (aField.getName().equals(bField.getName()) == false) {
            throw new FieldNameMatcherException("Field names not matching [ " + aField.getName() + ", " + bField.getName() + "]");
        }
        if (aField.getValue().equals(bField.getValue()) == false) {
            throw new FieldValeMatcherException("Field values not matching [" + aField.getValue()+ ", " + bField.getValue() + "]");
        }
    }
}
