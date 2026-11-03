/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

/**
 * Marks a physical exec that a detached {@code _doc} column can pass through, each row keeping the {@code _doc} of its
 * document. {@code _doc} is detached once it left the data node that read the documents, as on the coordinator. It then
 * names each document by its origin, not by a shard of the node that runs the exec. A projection may also drop the
 * column.
 * <p>
 * It is the coordinator counterpart of {@link org.elasticsearch.xpack.esql.plan.logical.CarriesDocOnDataNodes}, and
 * opt-in like it. Without the marker the query loads its columns before the exec, as it does without the fetch phase,
 * which is always correct. Add it only once a test shows that the fetch phase returns the same rows through the exec.
 */
public interface CarriesDetachedDoc {}
