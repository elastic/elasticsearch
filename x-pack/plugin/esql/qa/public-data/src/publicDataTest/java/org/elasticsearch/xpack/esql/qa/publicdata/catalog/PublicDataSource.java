/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.List;

/**
 * One logical public corpus (e.g. ClickBench {@code hits}), its provenance, and every
 * {@link SourceVariant} of it the suite can query. Every csv-spec query uses a {@code {{<id>}}} template
 * that {@link org.elasticsearch.xpack.esql.qa.publicdata.PublicDataSpecTestCase} resolves to a variant's
 * real resource; each {@link SourceVariant} declares its own {@link SourceVariant#specResource()} (see its
 * Javadoc for why one source can span more than one csv-spec file).
 *
 * @param id                stable identifier: the {@code {{id}}} template name and the
 *                           {@code -Dtests.public_data.source} filter value
 * @param displayName        human-readable name for the coverage report
 * @param homepage            the upstream publisher's documentation/homepage URL
 * @param license             the upstream license/terms of use, for provenance
 * @param queryProvenance     where the csv-spec queries came from: a published query set translated to
 *                            ES|QL, or "modelled on ClickBench" when none existed (plan section 5)
 * @param variants            every variant of this source the suite can query
 */
public record PublicDataSource(
    String id,
    String displayName,
    String homepage,
    String license,
    String queryProvenance,
    List<SourceVariant> variants
) {}
