/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageApplicationPrivileges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class GetUserPrivilegesResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final GetUserPrivilegesResponse original = randomResponse();

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);


        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
        final GetUserPrivilegesResponse copy = new GetUserPrivilegesResponse(in);

        assertThat(copy.getClusterPrivileges(), equalTo(original.getClusterPrivileges()));
        assertThat(copy.getConditionalClusterPrivileges(), equalTo(original.getConditionalClusterPrivileges()));
        assertThat(sorted(copy.getIndexPrivileges()), equalTo(sorted(original.getIndexPrivileges())));
        assertThat(copy.getApplicationPrivileges(), equalTo(original.getApplicationPrivileges()));
        assertThat(copy.getRunAs(), equalTo(original.getRunAs()));
    }

    public void testEqualsAndHashCode() throws IOException {
        final GetUserPrivilegesResponse response = randomResponse();
        final EqualsHashCodeTestUtils.CopyFunction<GetUserPrivilegesResponse> copy = original -> new GetUserPrivilegesResponse(
            original.getClusterPrivileges(),
            original.getConditionalClusterPrivileges(),
            original.getIndexPrivileges(),
            original.getApplicationPrivileges(),
            original.getRunAs()
        );
        final EqualsHashCodeTestUtils.MutateFunction<GetUserPrivilegesResponse> mutate =
            new EqualsHashCodeTestUtils.MutateFunction<GetUserPrivilegesResponse>() {
                @Override
                public GetUserPrivilegesResponse mutate(GetUserPrivilegesResponse original) {
                    final int random = randomIntBetween(1, 0b11111);
                    final Set<String> cluster = maybeMutate(random, 0, original.getClusterPrivileges(), () -> randomAlphaOfLength(5));
                    final Set<ConfigurableClusterPrivilege> conditionalCluster = maybeMutate(random, 1,
                        original.getConditionalClusterPrivileges(), () -> new ManageApplicationPrivileges(randomStringSet(3)));
                        final Set<GetUserPrivilegesResponse.Indices> index = maybeMutate(random, 2, original.getIndexPrivileges(),
                                () -> new GetUserPrivilegesResponse.Indices(randomStringSet(1), randomStringSet(1), emptySet(), emptySet(),
                                        randomBoolean()));
                    final Set<ApplicationResourcePrivileges> application = maybeMutate(random, 3, original.getApplicationPrivileges(),
                        () -> ApplicationResourcePrivileges.builder().resources(generateRandomStringArray(3, 3, false, false))
                            .application(randomAlphaOfLength(5)).privileges(generateRandomStringArray(3, 5, false, false)).build());
                    final Set<String> runAs = maybeMutate(random, 4, original.getRunAs(), () -> randomAlphaOfLength(8));
                    return new GetUserPrivilegesResponse(cluster, conditionalCluster, index, application, runAs);
                }

                private <T> Set<T> maybeMutate(int random, int index, Set<T> original, Supplier<T> supplier) {
                    if ((random & (1 << index)) == 0) {
                        return original;
                    }
                    if (original.isEmpty()) {
                        return Collections.singleton(supplier.get());
                    } else {
                        return emptySet();
                    }
                }
            };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, copy, mutate);
    }

    private GetUserPrivilegesResponse randomResponse() {
        final Set<String> cluster = randomStringSet(5);
        final Set<ConfigurableClusterPrivilege> conditionalCluster = Sets.newHashSet(randomArray(3, ConfigurableClusterPrivilege[]::new,
            () -> new ManageApplicationPrivileges(
                randomStringSet(3)
            )));
        final Set<GetUserPrivilegesResponse.Indices> index = Sets.newHashSet(randomArray(5, GetUserPrivilegesResponse.Indices[]::new,
            () -> new GetUserPrivilegesResponse.Indices(randomStringSet(6), randomStringSet(8),
                Sets.newHashSet(randomArray(3, FieldGrantExcludeGroup[]::new, () -> new FieldGrantExcludeGroup(
                    generateRandomStringArray(3, 5, false, false), generateRandomStringArray(3, 5, false, false)))),
                randomStringSet(3).stream().map(BytesArray::new).collect(Collectors.toSet()), randomBoolean()
            ))
        );
        final Set<ApplicationResourcePrivileges> application = Sets.newHashSet(randomArray(5, ApplicationResourcePrivileges[]::new,
            () -> ApplicationResourcePrivileges.builder().resources(generateRandomStringArray(3, 3, false, false))
                .application(randomAlphaOfLength(5)).privileges(generateRandomStringArray(3, 5, false, false)).build()
        ));
        final Set<String> runAs = randomStringSet(3);
        return new GetUserPrivilegesResponse(cluster, conditionalCluster, index, application, runAs);
    }

    private List<GetUserPrivilegesResponse.Indices> sorted(Collection<GetUserPrivilegesResponse.Indices> indices) {
        final ArrayList<GetUserPrivilegesResponse.Indices> list = CollectionUtils.iterableAsArrayList(indices);
        Collections.sort(list, (a, b) -> {
            int cmp = compareCollection(a.getIndices(), b.getIndices(), String::compareTo);
            if (cmp != 0) {
                return cmp;
            }
            cmp = compareCollection(a.getPrivileges(), b.getPrivileges(), String::compareTo);
            if (cmp != 0) {
                return cmp;
            }
            cmp = compareCollection(a.getQueries(), b.getQueries(), BytesReference::compareTo);
            if (cmp != 0) {
                return cmp;
            }
            cmp = compareCollection(a.getFieldSecurity(), b.getFieldSecurity(), (f1, f2) -> {
                int c = compareCollection(Arrays.asList(f1.getGrantedFields()), Arrays.asList(f2.getGrantedFields()), String::compareTo);
                if (c == 0) {
                    c = compareCollection(Arrays.asList(f1.getExcludedFields()), Arrays.asList(f2.getExcludedFields()), String::compareTo);
                }
                return c;
            });
            return cmp;
        });
        return list;
    }

    private <T> int compareCollection(Collection<T> a, Collection<T> b, Comparator<T> comparator) {
        int cmp = Integer.compare(a.size(), b.size());
        if (cmp != 0) {
            return cmp;
        }
        Iterator<T> i1 = a.iterator();
        Iterator<T> i2 = b.iterator();
        while (i1.hasNext()) {
            cmp = comparator.compare(i1.next(), i2.next());
            if (cmp != 0) {
                return cmp;
            }
        }
        return cmp;
    }

    private HashSet<String> randomStringSet(int maxSize) {
        return Sets.newHashSet(generateRandomStringArray(maxSize, randomIntBetween(3, 6), false, false));
    }
}
