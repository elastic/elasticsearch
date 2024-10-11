/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DatabaseTests extends ESTestCase {

    public void testDatabasePropertyInvariants() {
        // the city database is like a specialization of the country database
        assertThat(Sets.difference(Database.Country.properties(), Database.City.properties()), is(empty()));
        assertThat(Sets.difference(Database.Country.defaultProperties(), Database.City.defaultProperties()), is(empty()));

        // the isp database is like a specialization of the asn database
        assertThat(Sets.difference(Database.Asn.properties(), Database.Isp.properties()), is(empty()));
        assertThat(Sets.difference(Database.Asn.defaultProperties(), Database.Isp.defaultProperties()), is(empty()));

        // the enterprise database is like these other databases joined together
        for (Database type : Set.of(
            Database.City,
            Database.Country,
            Database.Asn,
            Database.AnonymousIp,
            Database.ConnectionType,
            Database.Domain,
            Database.Isp
        )) {
            assertThat(Sets.difference(type.properties(), Database.Enterprise.properties()), is(empty()));
        }
        // but in terms of the default fields, it's like a drop-in replacement for the city database
        // n.b. this is just a choice we decided to make here at Elastic
        assertThat(Database.Enterprise.defaultProperties(), equalTo(Database.City.defaultProperties()));
    }
}
