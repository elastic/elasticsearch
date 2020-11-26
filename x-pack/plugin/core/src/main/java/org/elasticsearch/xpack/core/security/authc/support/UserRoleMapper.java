/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.util.LDAPSDKUsageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Where a realm users an authentication method that does not have in-built support for X-Pack
 * {@link Role roles}, it may delegate to an implementation of this class the
 * responsibility for determining the set roles that an authenticated user should have.
 */
public interface UserRoleMapper {
    /**
     * Determines the set of roles that should be applied to <code>user</code>.
     */
    void resolveRoles(UserData user, ActionListener<Set<String>> listener);

    /**
     * Informs the mapper that the provided <code>realm</code> should be refreshed when
     * the set of role-mappings change. The realm may be updated for the local node only, or across
     * the whole cluster depending on whether this role-mapper has node-local data or cluster-wide
     * data.
     */
    void refreshRealmOnChange(CachingRealm realm);

    /**
     * A representation of a user for whom roles should be mapped.
     * The user has been authenticated, but does not yet have any roles.
     */
    class UserData {
        private final String username;
        @Nullable
        private final String dn;
        private final Set<String> groups;
        private final Map<String, Object> metadata;
        private final RealmConfig realm;

        public UserData(String username, @Nullable String dn, Collection<String> groups,
                        Map<String, Object> metadata, RealmConfig realm) {
            this.username = username;
            this.dn = dn;
            this.groups = Set.copyOf(groups);
            this.metadata = Map.copyOf(metadata);
            this.realm = realm;
        }

        /**
         * Formats the user data as a {@link ExpressionModel}.
         * The model does <em>not</em> have nested values - all values are simple Java values, but keys may
         * contain <code>.</code>.
         * For example, the {@link #metadata} values will be stored in the model with a key of
         * <code>"metadata.KEY"</code> where <code>KEY</code> is the key from the metadata object.
         */
        public ExpressionModel asModel() {
            final ExpressionModel model = new ExpressionModel();
            model.defineField("username", username);
            if (dn != null) {
                // null dn fields get the default NULL_PREDICATE
                model.defineField("dn", dn, new DistinguishedNamePredicate(dn));
            }
            model.defineField("groups", groups, groups.stream()
                    .<Predicate<FieldExpression.FieldValue>>map(DistinguishedNamePredicate::new)
                    .reduce(Predicate::or)
                    .orElse(fieldValue -> false)
            );
            metadata.keySet().forEach(k -> model.defineField("metadata." + k, metadata.get(k)));
            model.defineField("realm.name", realm.name());
            return model;
        }

        @Override
        public String toString() {
            return "UserData{" +
                    "username:" + username +
                    "; dn:" + dn +
                    "; groups:" + groups +
                    "; metadata:" + metadata +
                    "; realm=" + realm.name() +
                    '}';
        }

        /**
         * The username for the authenticated user.
         */
        public String getUsername() {
            return username;
        }

        /**
         * The <em>distinguished name</em> of the authenticated user, if applicable to the
         * authentication method used. Otherwise, <code>null</code>.
         */
        @Nullable
        public String getDn() {
            return dn;
        }

        /**
         * The groups to which the user belongs in the originating user store. Should be empty
         * if the user store or authentication method does not support groups.
         */
        public Set<String> getGroups() {
            return groups;
        }

        /**
         * Any additional metadata that was provided at authentication time. The set of keys will
         * vary according to the authenticating realm.
         */
        public Map<String, Object> getMetadata() {
            return metadata;
        }

        /**
         * The realm that authenticated the user.
         */
        public RealmConfig getRealm() {
            return realm;
        }
    }

    /**
     * A specialised predicate for fields that might be a DistinguishedName (e.g "dn" or "groups").
     *
     * The X500 specs define how to compare DistinguishedNames (but we mostly rely on {@link DN#equals(Object)}),
     * which means "CN=me,DC=example,DC=com" should be equal to "cn=me, dc=Example, dc=COM" (and other variations).

     * The {@link FieldExpression} class doesn't know about special rules for special data types, but the
     * {@link ExpressionModel} class can take a custom {@code Predicate} that tests whether the data in the model
     * matches the {@link FieldExpression.FieldValue value} in the expression.
     *
     * The string constructor parameter may or may not actually parse as a DN - the "dn" field <em>should</em>
     * always be a DN, however groups will be a DN if they're from an LDAP/AD realm, but often won't be for a SAML realm.
     *
     * Because the {@link FieldExpression.FieldValue} might be a pattern ({@link CharacterRunAutomaton automaton}),
     * we sometimes need to do more complex matching than just comparing a DN for equality.
     *
     */
    class DistinguishedNamePredicate implements Predicate<FieldExpression.FieldValue> {
        private static final Logger LOGGER = LogManager.getLogger(DistinguishedNamePredicate.class);

        private final String string;
        private final DN dn;

        public DistinguishedNamePredicate(String string) {
            assert string != null : "DN string should not be null. Use the dedicated NULL_PREDICATE for every user null field.";
            this.string = string;
            this.dn = parseDn(string);
        }

        private static DN parseDn(String string) {
            try {
                return new DN(string);
            } catch (LDAPException | LDAPSDKUsageException e) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(new ParameterizedMessage("failed to parse [{}] as a DN", string), e);
                }
                return null;
            }
        }

        @Override
        public String toString() {
            return string;
        }

        @Override
        public boolean test(FieldExpression.FieldValue fieldValue) {
            final CharacterRunAutomaton automaton = fieldValue.getAutomaton();
            if (automaton != null) {
                if (automaton.run(string)) {
                    return true;
                }
                if (dn != null && automaton.run(dn.toNormalizedString())) {
                    return true;
                }
                if (automaton.run(string.toLowerCase(Locale.ROOT)) || automaton.run(string.toUpperCase(Locale.ROOT))) {
                    return true;
                }
                if (dn == null) {
                    return false;
                }

                assert fieldValue.getValue() instanceof String : "FieldValue " + fieldValue + " has automaton but value is "
                        + (fieldValue.getValue() == null ? "<null>" : fieldValue.getValue().getClass());
                String pattern = (String) fieldValue.getValue();

                // If the pattern is "*,dc=example,dc=com" then the rule is actually trying to express a DN sub-tree match.
                // We can use dn.isDescendantOf for that
                if (pattern.startsWith("*,")) {
                    final String suffix = pattern.substring(2);
                    // if the suffix has a wildcard, then it's not a pure sub-tree match
                    if (suffix.indexOf('*') == -1) {
                        final DN dnSuffix = parseDn(suffix);
                        if (dnSuffix != null && dn.isDescendantOf(dnSuffix, false)) {
                            return true;
                        }
                    }
                }

                return false;
            }
            if (fieldValue.getValue() instanceof String) {
                final String testString = (String) fieldValue.getValue();
                if (testString.equalsIgnoreCase(string)) {
                    return true;
                }
                if (dn == null) {
                    return false;
                }

                final DN testDn = parseDn(testString);
                if (testDn != null) {
                    return dn.equals(testDn);
                }
                return testString.equalsIgnoreCase(dn.toNormalizedString());
            }
            return false;
        }
    }
}
