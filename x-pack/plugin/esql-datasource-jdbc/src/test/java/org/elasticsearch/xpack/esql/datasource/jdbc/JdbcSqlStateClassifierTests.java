/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.sql.SQLException;

import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.AUTH_FAILED;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.CANCELLED_BY_USER;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.DATA_ERROR;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.DEADLOCK;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.INTEGRITY_VIOLATION;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.PERMISSION;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.RESOURCE_EXHAUSTED;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.SYNTAX_ERROR;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.TRANSIENT_NETWORK;
import static org.elasticsearch.xpack.esql.datasource.jdbc.JdbcSqlStateCategory.UNKNOWN;

/**
 * Full category matrix for {@link JdbcSqlStateClassifier}: representative PostgreSQL / MySQL / H2 / Amazon Redshift
 * {@code SQLState}s and vendor codes for every {@link JdbcSqlStateCategory}, plus the notable edge cases
 * (null/blank {@code SQLState} → {@link JdbcSqlStateCategory#UNKNOWN UNKNOWN}; a runtime exception wrapping a
 * {@link SQLException} two levels deep; {@code 08P01} deliberately NOT transient; cause-chain cycles terminate).
 * <p>
 * Vendor-code sources are cited in {@link JdbcSqlStateClassifier}'s Javadoc. A "should not retry" assertion for each
 * non-retryable category (i.e. it is classified as something other than AUTH_FAILED / TRANSIENT_NETWORK) lives here;
 * the end-to-end "propagates on the first attempt" behavior is verified against {@link JdbcConnector} in
 * {@link JdbcTransientRetryTests}.
 */
public class JdbcSqlStateClassifierTests extends ESTestCase {

    private static SQLException sqlState(String sqlState) {
        return new SQLException("boom", sqlState);
    }

    private static SQLException sqlState(String sqlState, int vendorCode) {
        return new SQLException("boom", sqlState, vendorCode);
    }

    private static void assertCategory(JdbcSqlStateCategory expected, String sqlState) {
        assertEquals("SQLState [" + sqlState + "]", expected, JdbcSqlStateClassifier.classify(sqlState(sqlState)));
    }

    // -- AUTH_FAILED (class 28) ----------------------------------------------------------------

    public void testAuthFailed() {
        assertCategory(AUTH_FAILED, "28000"); // SQL standard invalid_authorization_specification (PG/MySQL/H2/Redshift)
        assertCategory(AUTH_FAILED, "28P01"); // PostgreSQL invalid_password
        // MySQL ER_ACCESS_DENIED_ERROR (1045) surfaces as SQLState 28000; the vendor code must not change the outcome.
        assertEquals(AUTH_FAILED, JdbcSqlStateClassifier.classify(sqlState("28000", 1045)));
    }

    // -- TRANSIENT_NETWORK (class 08, except 08P01) --------------------------------------------

    public void testTransientNetwork() {
        for (String s : new String[] { "08000", "08001", "08003", "08004", "08006", "08007" }) {
            assertCategory(TRANSIENT_NETWORK, s); // PostgreSQL / Redshift connection-exception family
        }
        assertCategory(TRANSIENT_NETWORK, "08S01"); // MySQL communication link failure
    }

    public void testProtocolViolationIsNotTransientAndNotRetryable() {
        // 08P01 (PostgreSQL protocol_violation) is a connection-class code that a retry will NOT fix. It must be
        // UNKNOWN (fail-fast), NOT TRANSIENT_NETWORK -- both case variants.
        assertCategory(UNKNOWN, "08P01");
        assertCategory(UNKNOWN, "08p01");
        assertNotRetryable("08P01");
    }

    // -- DEADLOCK (40001 / 40P01 / MySQL 1213, 1205) -------------------------------------------

    public void testDeadlock() {
        assertCategory(DEADLOCK, "40001"); // serialization_failure (PostgreSQL / Redshift / MySQL 1213 SQLState)
        assertCategory(DEADLOCK, "40P01"); // PostgreSQL deadlock_detected
        // MySQL InnoDB lock-contention codes resolved via the vendor-code fallback when the SQLState is generic.
        assertEquals(DEADLOCK, JdbcSqlStateClassifier.classify(sqlState("HY000", 1213))); // ER_LOCK_DEADLOCK
        assertEquals(DEADLOCK, JdbcSqlStateClassifier.classify(sqlState("HY000", 1205))); // ER_LOCK_WAIT_TIMEOUT
        // A recognized SQLState is not overridden by an unrelated vendor code.
        assertEquals(DEADLOCK, JdbcSqlStateClassifier.classify(sqlState("40001", 1213)));
    }

    // -- RESOURCE_EXHAUSTED (class 53) ---------------------------------------------------------

    public void testResourceExhausted() {
        assertCategory(RESOURCE_EXHAUSTED, "53000"); // insufficient_resources
        assertCategory(RESOURCE_EXHAUSTED, "53100"); // disk_full
        assertCategory(RESOURCE_EXHAUSTED, "53200"); // out_of_memory
        assertCategory(RESOURCE_EXHAUSTED, "53300"); // too_many_connections (common on Redshift WLM saturation)
    }

    // -- SYNTAX_ERROR (class 42, excluding the PERMISSION carve-outs) --------------------------

    public void testSyntaxError() {
        assertCategory(SYNTAX_ERROR, "42601"); // PostgreSQL / Redshift syntax_error
        assertCategory(SYNTAX_ERROR, "42P01"); // PostgreSQL undefined_table
        assertCategory(SYNTAX_ERROR, "42000"); // MySQL / H2 syntax error or access rule violation
        assertCategory(SYNTAX_ERROR, "42S02"); // MySQL base table or view not found
    }

    // -- DATA_ERROR (class 22) -----------------------------------------------------------------

    public void testDataError() {
        assertCategory(DATA_ERROR, "22P02"); // PostgreSQL invalid_text_representation
        assertCategory(DATA_ERROR, "22003"); // numeric_value_out_of_range (PG / Redshift)
        assertCategory(DATA_ERROR, "22007"); // invalid_datetime_format
        assertCategory(DATA_ERROR, "22018"); // H2 data conversion error / invalid character value for cast
    }

    // -- INTEGRITY_VIOLATION (class 23) --------------------------------------------------------

    public void testIntegrityViolation() {
        assertCategory(INTEGRITY_VIOLATION, "23505"); // unique_violation (PG / Redshift)
        assertCategory(INTEGRITY_VIOLATION, "23503"); // foreign_key_violation
        assertCategory(INTEGRITY_VIOLATION, "23000"); // MySQL integrity constraint violation
    }

    // -- PERMISSION (42501 / 42P02) ------------------------------------------------------------

    public void testPermission() {
        assertCategory(PERMISSION, "42501"); // PostgreSQL / Redshift insufficient_privilege
        assertCategory(PERMISSION, "42P02"); // grouped under PERMISSION (non-retryable, fail-fast)
        // The carve-outs must win over the generic class-42 -> SYNTAX_ERROR mapping.
        assertNotEquals(SYNTAX_ERROR, JdbcSqlStateClassifier.classify(sqlState("42501")));
    }

    // -- CANCELLED_BY_USER (57014) -------------------------------------------------------------

    public void testCancelledByUser() {
        assertCategory(CANCELLED_BY_USER, "57014"); // PostgreSQL / Redshift query_canceled
    }

    // -- UNKNOWN / edge cases ------------------------------------------------------------------

    public void testNullSqlStateIsUnknown() {
        assertEquals(UNKNOWN, JdbcSqlStateClassifier.classify(new SQLException("no state")));
        assertEquals(UNKNOWN, JdbcSqlStateClassifier.classify(sqlState(null)));
    }

    public void testBlankSqlStateIsUnknown() {
        assertCategory(UNKNOWN, "");
        assertCategory(UNKNOWN, "   ");
    }

    public void testUnrecognizedSqlStateIsUnknown() {
        assertCategory(UNKNOWN, "99999");
        assertCategory(UNKNOWN, "XX000"); // PostgreSQL internal_error
        assertCategory(UNKNOWN, "90005"); // an H2-internal error code
        assertCategory(UNKNOWN, "HY000"); // generic SQLState with no recognized vendor code
    }

    public void testNullThrowableIsUnknown() {
        assertEquals(UNKNOWN, JdbcSqlStateClassifier.classify((Throwable) null));
        assertEquals(UNKNOWN, JdbcSqlStateClassifier.classify((SQLException) null));
    }

    public void testCaseInsensitiveSqlState() {
        assertCategory(DEADLOCK, "40p01");
        assertCategory(TRANSIENT_NETWORK, "08S01".toLowerCase(java.util.Locale.ROOT));
        assertCategory(PERMISSION, "42p02");
    }

    // -- cause-chain walking -------------------------------------------------------------------

    public void testRuntimeExceptionWrappingSqlExceptionTwoLevelsDeep() {
        // A driver/framework wraps the real SQLException inside runtime exceptions; the classifier must dig it out.
        SQLException real = sqlState("28P01");
        RuntimeException wrapped = new RuntimeException("outer", new IllegalStateException("middle", real));
        assertEquals(AUTH_FAILED, JdbcSqlStateClassifier.classify(wrapped));
    }

    public void testSqlExceptionCauseChainWalked() {
        SQLException inner = sqlState("08006"); // connection_failure
        SQLException outer = new SQLException("wrapper with no useful state"); // null SQLState
        outer.initCause(inner);
        assertEquals(TRANSIENT_NETWORK, JdbcSqlStateClassifier.classify(outer));
    }

    public void testNextExceptionChainWalked() {
        // Some drivers chain via getNextException() rather than getCause().
        SQLException head = new SQLException("head, no state");
        SQLException next = sqlState("53300");
        head.setNextException(next);
        assertEquals(RESOURCE_EXHAUSTED, JdbcSqlStateClassifier.classify(head));
    }

    public void testOutermostRecognizedCategoryWins() {
        // Outer carries a recognized state; a differently-categorized inner cause must not override it.
        SQLException inner = sqlState("08006"); // would be TRANSIENT
        SQLException outer = sqlState("28000");  // AUTH -- outermost recognized wins
        outer.initCause(inner);
        assertEquals(AUTH_FAILED, JdbcSqlStateClassifier.classify(outer));
    }

    public void testSelfReferentialNextExceptionTerminates() {
        // A driver that sets e.getNextException() == e must not spin the classifier.
        SQLException e = sqlState("99999");
        e.setNextException(e);
        assertEquals(UNKNOWN, JdbcSqlStateClassifier.classify(e));
    }

    public void testMutuallyReferentialNextExceptionTerminates() {
        // e1.next == e2, e2.next == e1: the shared visited set makes the BFS terminate (no StackOverflow), and the
        // recognized state on e2 is still found.
        SQLException e1 = new SQLException("no state");
        SQLException e2 = sqlState("40001");
        e1.setNextException(e2);
        e2.setNextException(e1);
        assertEquals(DEADLOCK, JdbcSqlStateClassifier.classify(e1));
    }

    // -- helper --------------------------------------------------------------------------------

    /** Asserts a SQLState is classified as a NON-retryable category (neither AUTH_FAILED nor TRANSIENT_NETWORK). */
    private static void assertNotRetryable(String sqlState) {
        JdbcSqlStateCategory c = JdbcSqlStateClassifier.classify(sqlState(sqlState));
        assertNotEquals("SQLState [" + sqlState + "] must not be AUTH_FAILED", AUTH_FAILED, c);
        assertNotEquals("SQLState [" + sqlState + "] must not be TRANSIENT_NETWORK", TRANSIENT_NETWORK, c);
    }

    public void testNonRetryableCategoriesAreNotRetryable() {
        // Every non-retryable category's representative SQLState must classify as neither retryable category.
        for (String s : new String[] {
            "40001",  // DEADLOCK
            "40P01",  // DEADLOCK
            "53300",  // RESOURCE_EXHAUSTED
            "42601",  // SYNTAX_ERROR
            "22P02",  // DATA_ERROR
            "23505",  // INTEGRITY_VIOLATION
            "42501",  // PERMISSION
            "57014",  // CANCELLED_BY_USER
            "08P01",  // protocol violation -> UNKNOWN
            "99999"   // UNKNOWN
        }) {
            assertNotRetryable(s);
        }
    }
}
