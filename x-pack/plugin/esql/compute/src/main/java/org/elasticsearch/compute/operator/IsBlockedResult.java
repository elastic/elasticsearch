/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;

import java.util.Map;

/**
 * Is this {@link Operator} blocked?
 * <p>
 *     If the {@link #listener}'s {@link SubscribableListener#isDone()} method
 *     returns {@code true} then the {@linkplain Operator} is not blocked.
 * </p>
 * <p>
 *     If the {@linkplain Operator} is blocked then you can
 *     {@link SubscribableListener#addListener} to the {@link #listener} to be
 *     notified when the {@linkplain Operator} is unblocked.
 * </p>
 * @param listener a listener to check for blocked-ness
 * @param reason the reason that the {@linkplain Operator} is blocked.
 *               This is used as a {@link Map} key so this shouldn't
 *               vary wildly, but it should be descriptive of the reason
 *               the operator went async.
 */
public record IsBlockedResult(SubscribableListener<Void> listener, String reason) {}
