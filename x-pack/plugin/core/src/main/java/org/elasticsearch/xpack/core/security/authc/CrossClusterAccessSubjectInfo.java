/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class CrossClusterAccessSubjectInfo {

    public static final String CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY = "_cross_cluster_access_subject_info";
    private static final Logger logger = LogManager.getLogger(CrossClusterAccessSubjectInfo.class);
    private static final Set<String> API_KEY_AUTHENTICATION_METADATA_TO_KEEP = Set.of(
        AuthenticationField.API_KEY_ID_KEY,
        // These are required for complete audit log entries
        AuthenticationField.API_KEY_NAME_KEY,
        AuthenticationField.API_KEY_CREATOR_REALM_NAME,
        AuthenticationField.API_KEY_CREATOR_REALM_TYPE
    );
    private static final Set<String> SERVICE_ACCOUNT_AUTHENTICATION_METADATA_TO_KEEP = Set.of(
        // These are required for complete audit log entries
        ServiceAccountSettings.TOKEN_NAME_FIELD,
        ServiceAccountSettings.TOKEN_SOURCE_FIELD
    );

    private final Authentication authentication;
    private final List<RoleDescriptorsBytes> roleDescriptorsBytesList;

    public CrossClusterAccessSubjectInfo(Authentication authentication, RoleDescriptorsIntersection roleDescriptorsIntersection)
        throws IOException {
        this(authentication, toRoleDescriptorsBytesList(roleDescriptorsIntersection));
    }

    private CrossClusterAccessSubjectInfo(Authentication authentication, List<RoleDescriptorsBytes> roleDescriptorsBytesList) {
        this.authentication = authentication;
        this.roleDescriptorsBytesList = roleDescriptorsBytesList;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY, encode());
    }

    public static CrossClusterAccessSubjectInfo readFromContext(final ThreadContext ctx) throws IOException {
        final String header = ctx.getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY);
        if (header == null) {
            throw new IllegalArgumentException(
                "cross cluster access header [" + CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY + "] is required"
            );
        }
        return decode(header);
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public CrossClusterAccessSubjectInfo cleanAndValidate() {
        // Need to do this first. Otherwise, the `copyWithFilteredMetadataFields` call in `copyAuthenticationWithCleanMetadata` may fail
        // with a confusing error message for unsupported types
        if (authentication.isCrossClusterAccess()) {
            final Subject effectiveSubject = authentication.getEffectiveSubject();
            throw new IllegalArgumentException(
                "subject ["
                    + effectiveSubject.getUser().principal()
                    + "] has type ["
                    + effectiveSubject.getType()
                    + "] but nested cross cluster access is not supported"
            );
        }
        final var cleanCopy = new CrossClusterAccessSubjectInfo(copyAuthenticationWithCleanMetadata(), roleDescriptorsBytesList);
        cleanCopy.validate();
        return cleanCopy;
    }

    public List<RoleDescriptorsBytes> getRoleDescriptorsBytesList() {
        return roleDescriptorsBytesList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CrossClusterAccessSubjectInfo that = (CrossClusterAccessSubjectInfo) o;

        if (false == authentication.equals(that.authentication)) return false;
        return roleDescriptorsBytesList.equals(that.roleDescriptorsBytesList);
    }

    @Override
    public int hashCode() {
        int result = authentication.hashCode();
        result = 31 * result + roleDescriptorsBytesList.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CrossClusterAccessSubjectInfo{"
            + "authentication="
            + authentication
            + ", roleDescriptorsBytesList="
            + roleDescriptorsBytesList
            + '}';
    }

    private static List<RoleDescriptorsBytes> toRoleDescriptorsBytesList(final RoleDescriptorsIntersection roleDescriptorsIntersection)
        throws IOException {
        // If we ever lift this restriction, we need to ensure that the serialization of each set of role descriptors to raw bytes is
        // deterministic. We can do so by sorting the role descriptors before serializing.
        assert roleDescriptorsIntersection.roleDescriptorsList().stream().noneMatch(rds -> rds.size() > 1)
            : "sets with more than one role descriptor are not supported for cross cluster access authentication";
        final List<RoleDescriptorsBytes> roleDescriptorsBytesList = new ArrayList<>();
        for (Set<RoleDescriptor> roleDescriptors : roleDescriptorsIntersection.roleDescriptorsList()) {
            roleDescriptorsBytesList.add(RoleDescriptorsBytes.fromRoleDescriptors(roleDescriptors));
        }
        return roleDescriptorsBytesList;
    }

    public String encode() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(authentication.getEffectiveSubject().getTransportVersion());
        TransportVersion.writeVersion(authentication.getEffectiveSubject().getTransportVersion(), out);
        authentication.writeTo(out);
        out.writeCollection(roleDescriptorsBytesList, (o, rdb) -> rdb.writeTo(o));
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
    }

    public static CrossClusterAccessSubjectInfo decode(final String header) throws IOException {
        Objects.requireNonNull(header);
        final byte[] bytes = Base64.getDecoder().decode(header);
        final StreamInput in = StreamInput.wrap(bytes);
        final TransportVersion version = TransportVersion.readVersion(in);
        in.setTransportVersion(version);
        final Authentication authentication = new Authentication(in);
        final List<RoleDescriptorsBytes> roleDescriptorsBytesList = in.readImmutableList(RoleDescriptorsBytes::new);
        return new CrossClusterAccessSubjectInfo(authentication, roleDescriptorsBytesList);
    }

    /**
     * Returns a copy of the passed-in metadata map, with the relevant cross cluster access fields included.
     * Does not modify the original map.
     */
    public Map<String, Object> copyWithCrossClusterAccessEntries(final Map<String, Object> authenticationMetadata) {
        assert false == authenticationMetadata.containsKey(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY)
            : "metadata already contains [" + AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY + "] entry";
        assert false == authenticationMetadata.containsKey(AuthenticationField.CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY)
            : "metadata already contains [" + AuthenticationField.CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY + "] entry";
        assert false == getAuthentication().isCrossClusterAccess()
            : "authentication included in cross cluster access header cannot itself be cross cluster access";
        final Map<String, Object> copy = new HashMap<>(authenticationMetadata);
        copy.put(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY, getAuthentication());
        copy.put(AuthenticationField.CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY, getRoleDescriptorsBytesList());
        return Collections.unmodifiableMap(copy);
    }

    private Authentication copyAuthenticationWithCleanMetadata() {
        assert false == authentication.isCrossClusterAccess();
        if (authentication.isAuthenticatedAsApiKey()) {
            return authentication.copyWithFilteredMetadataFields(API_KEY_AUTHENTICATION_METADATA_TO_KEEP);
        } else if (authentication.isServiceAccount()) {
            return authentication.copyWithFilteredMetadataFields(SERVICE_ACCOUNT_AUTHENTICATION_METADATA_TO_KEEP);
        } else {
            return authentication.copyWithEmptyMetadata();
        }
    }

    private void validate() {
        assert false == authentication.isCrossClusterAccess();
        authentication.checkConsistency();
        final User user = authentication.getEffectiveSubject().getUser();
        if (user == InternalUsers.SYSTEM_USER) {
            if (false == getRoleDescriptorsBytesList().isEmpty()) {
                logger.warn(
                    "Received non-empty remote access role descriptors bytes list for _system user. "
                        + "These will be ignored during authorization."
                );
                assert false : "role descriptors bytes list for internal cross cluster access user must be empty";
            }
        } else if (user instanceof InternalUser) {
            throw new IllegalArgumentException(
                "received cross cluster request from an unexpected internal user [" + user.principal() + "]"
            );
        }
    }

    public static final class RoleDescriptorsBytes implements Writeable {

        public static final RoleDescriptorsBytes EMPTY = new RoleDescriptorsBytes(new BytesArray("{}"));
        private final BytesReference rawBytes;

        public RoleDescriptorsBytes(BytesReference rawBytes) {
            this.rawBytes = rawBytes;
        }

        public RoleDescriptorsBytes(StreamInput streamInput) throws IOException {
            this(streamInput.readBytesReference());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(rawBytes);
        }

        /**
         * Compute the sha256 digest of the bytes
         * @return A hexadecimal representation of the sha256 digest
         */
        public String digest() {
            return MessageDigests.toHexString(MessageDigests.digest(rawBytes, MessageDigests.sha256()));
        }

        public static RoleDescriptorsBytes fromRoleDescriptors(final Set<RoleDescriptor> roleDescriptors) throws IOException {
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for (RoleDescriptor roleDescriptor : roleDescriptors) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
            return new RoleDescriptorsBytes(BytesReference.bytes(builder));
        }

        public Set<RoleDescriptor> toRoleDescriptors() {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, rawBytes, XContentType.JSON)) {
                final List<RoleDescriptor> roleDescriptors = new ArrayList<>();
                parser.nextToken();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    parser.nextToken();
                    final String roleName = parser.currentName();
                    roleDescriptors.add(RoleDescriptor.parse(roleName, parser, false));
                }
                return Set.copyOf(roleDescriptors);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RoleDescriptorsBytes that = (RoleDescriptorsBytes) o;
            return Objects.equals(rawBytes, that.rawBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rawBytes);
        }

        @Override
        public String toString() {
            return "RoleDescriptorsBytes{" + "rawBytes=" + rawBytes + '}';
        }
    }
}
