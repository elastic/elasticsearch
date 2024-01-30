/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;

public class EnrichHelper {

    static String randomEnrichCommand(EnrichPolicy policy, String name, Enrich.Mode mode) {
        String onField = " ";
        String withFields = " ";

        List<String> before = new ArrayList<>();
        List<String> after = new ArrayList<>();
        String matchField = policy.getMatchField();

        if (randomBoolean()) {
            // => RENAME new_match_field=match_field | ENRICH name ON new_match_field | RENAME new_match_field AS match_field
            String newMatchField = "my_" + policy.getMatchField();
            before.add("RENAME " + matchField + " AS " + newMatchField);
            onField = " ON " + newMatchField;
            after.add("RENAME " + newMatchField + " AS " + matchField);
        } else if (randomBoolean()) {
            onField = " ON " + matchField;
        }
        if (randomBoolean()) {
            List<String> fields = new ArrayList<>();
            for (String f : policy.getEnrichFields()) {
                if (randomBoolean()) {
                    fields.add(f);
                } else {
                    // ENRICH name WITH new_a=a,b|new_c=c | RENAME new_a AS a | RENAME new_c AS c
                    fields.add("new_" + f + "=" + f);
                    after.add("RENAME new_" + f + " AS " + f);
                }
            }
            withFields = " WITH " + String.join(",", fields);
        }
        String enrich = "ENRICH";
        if (mode != Enrich.Mode.ANY || randomBoolean()) {
            enrich += " [ccq.mode: " + mode + "] ";
        }
        enrich += " " + name;
        enrich += onField;
        enrich += withFields;
        List<String> all = new ArrayList<>(before);
        all.add(enrich);
        all.addAll(after);
        return String.join(" | ", all);
    }
}
