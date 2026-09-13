/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.List;

/**
 * A declared coverage gap. Every matrix dimension value not covered by an active variant must be
 * either structurally {@code blocked} (derived, e.g. HTTPS cannot list) or declared here — so every
 * hole in the matrix is self-documenting and the validator can require it.
 *
 * @param id     short kebab-case identifier, e.g. {@code gcs-provider}
 * @param reason why the cell is uncovered and what would close it
 * @param cells  the dimension values this gap covers, as {@code dimension=value} strings, e.g.
 *               {@code provider=gcs}, {@code codec=snappy}, {@code layout=nested_hive}
 */
public record GapSpec(String id, String reason, List<String> cells) {}
