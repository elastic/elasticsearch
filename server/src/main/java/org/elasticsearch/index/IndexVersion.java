/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Assertions;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * The index version.
 * <p>
 * Prior to 8.8.0, the node {@link Version} was used everywhere. This class separates the index format version
 * from the running node version.
 * <p>
 * Each index version constant has an id number, which for versions prior to 8.9.0 is the same as the release version
 * for backwards compatibility. In 8.9.0 this is changed to an incrementing number, disconnected from the release version.
 * <p>
 * Each version constant has a unique id string. This is not actually stored in the index, but is there to ensure
 * each index version is only added to the source file once. This string needs to be unique (normally a UUID,
 * but can be any other unique nonempty string).
 * If two concurrent PRs add the same index version, the different unique ids cause a git conflict, ensuring the second PR to be merged
 * must be updated with the next free version first. Without the unique id string, git will happily merge the two versions together,
 * resulting in the same index version being used across multiple commits,
 * causing problems when you try to upgrade between those two merged commits.
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link #MINIMUM_COMPATIBLE} field. Previously, this was dynamically calculated
 * from the major/minor versions of {@link Version}, but {@code IndexVersion} does not have separate major/minor version numbers.
 * So the minimum compatible version is hard-coded as the index version used by the first version of the previous major release.
 * {@link #MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <h2>Adding a new version</h2>
 * A new index version should be added <em>every time</em> a change is made to the serialization protocol of one or more classes.
 * Each index version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new index version, add a new constant at the bottom of the list that is one greater than the current highest version,
 * ensure it has a unique id, and update the {@link #CURRENT} constant to point to the new version.
 * <h2>Reverting an index version</h2>
 * If you revert a commit with an index version change, you <em>must</em> ensure there is a <em>new</em> index version
 * representing the reverted change. <em>Do not</em> let the index version go backwards, it must <em>always</em> be incremented.
 */
@SuppressWarnings({"checkstyle:linelength", "deprecation"})
public record IndexVersion(int id, Version luceneVersion) implements Comparable<IndexVersion> {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerIndexVersion method.
     * When all the transport version constants have been registered, the map is cleared & never touched again.
     */
    private static Map<String, Integer> IDS = new HashMap<>();

    private static IndexVersion registerIndexVersion(int id, Version luceneVersion, String uniqueId) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        Strings.requireNonEmpty(uniqueId, "Each IndexVersion needs a unique string id");
        Integer existing = IDS.put(uniqueId, id);
        if (existing != null) {
            throw new IllegalArgumentException("Versions " + id + " and " + existing + " have the same unique id");
        }
        return new IndexVersion(id, luceneVersion);
    }

    public static final IndexVersion ZERO = registerIndexVersion(0, Version.LATEST, "00000000-0000-0000-0000-000000000000");
    public static final IndexVersion V_7_0_0 = registerIndexVersion(7_00_00_99, Version.LUCENE_8_0_0, "b32be92d-c403-4858-a4a3-20d699a47ae6");
    public static final IndexVersion V_7_0_1 = registerIndexVersion(7_00_01_99, Version.LUCENE_8_0_0, "a03ed728-eac8-4e50-bcce-864806bb10e0");
    public static final IndexVersion V_7_1_0 = registerIndexVersion(7_01_00_99, Version.LUCENE_8_0_0, "f9964d87-9f20-4b26-af32-be1f979216ec");
    public static final IndexVersion V_7_1_1 = registerIndexVersion(7_01_01_99, Version.LUCENE_8_0_0, "29a3fb69-55d0-4389-aea9-96c98ce23830");
    public static final IndexVersion V_7_2_0 = registerIndexVersion(7_02_00_99, Version.LUCENE_8_0_0, "dba49448-87d4-45bb-ba19-f7b4eb85c757");
    public static final IndexVersion V_7_2_1 = registerIndexVersion(7_02_01_99, Version.LUCENE_8_0_0, "58874b45-f9f8-4c04-92a9-67548a8b21c3");
    public static final IndexVersion V_7_3_0 = registerIndexVersion(7_03_00_99, Version.LUCENE_8_1_0, "3d8a21df-58a4-4d7a-ba5d-438c92c16a7b");
    public static final IndexVersion V_7_3_1 = registerIndexVersion(7_03_01_99, Version.LUCENE_8_1_0, "5687797f-448b-490d-94d4-d7e8cfac0c98");
    public static final IndexVersion V_7_3_2 = registerIndexVersion(7_03_02_99, Version.LUCENE_8_1_0, "5a3462e5-d2fe-4b7b-9a7e-c0234412271f");
    public static final IndexVersion V_7_4_0 = registerIndexVersion(7_04_00_99, Version.LUCENE_8_2_0, "c1fe73ba-0173-476c-aba2-855c2b31ac18");
    public static final IndexVersion V_7_4_1 = registerIndexVersion(7_04_01_99, Version.LUCENE_8_2_0, "8a917374-bd4f-45e3-9052-575c4cf741cd");
    public static final IndexVersion V_7_4_2 = registerIndexVersion(7_04_02_99, Version.LUCENE_8_2_0, "f073a867-cba2-41e4-8150-a2f2a96f1e0b");
    public static final IndexVersion V_7_5_0 = registerIndexVersion(7_05_00_99, Version.LUCENE_8_3_0, "ab08ae25-ede2-4e57-a43f-89d96aa989e4");
    public static final IndexVersion V_7_5_1 = registerIndexVersion(7_05_01_99, Version.LUCENE_8_3_0, "a386d62e-cb85-4a37-b5f9-c9468bbfc457");
    public static final IndexVersion V_7_5_2 = registerIndexVersion(7_05_02_99, Version.LUCENE_8_3_0, "706715ca-3b91-40d2-8c2e-c34c459b5d0d");
    public static final IndexVersion V_7_6_0 = registerIndexVersion(7_06_00_99, Version.LUCENE_8_4_0, "63acbdb9-51c8-4976-bb3d-e55052a4fbd4");
    public static final IndexVersion V_7_6_1 = registerIndexVersion(7_06_01_99, Version.LUCENE_8_4_0, "1acc33d3-28dc-448d-953a-664dad3bf1f5");
    public static final IndexVersion V_7_6_2 = registerIndexVersion(7_06_02_99, Version.LUCENE_8_4_0, "3aa17069-fa04-4bf9-96af-fe8b903faa75");
    public static final IndexVersion V_7_7_0 = registerIndexVersion(7_07_00_99, Version.LUCENE_8_5_1, "6fff8238-e6ce-4fb2-85de-196492026e49");
    public static final IndexVersion V_7_7_1 = registerIndexVersion(7_07_01_99, Version.LUCENE_8_5_1, "4ce6641d-157b-4c59-8261-7997ac0f6e40");
    public static final IndexVersion V_7_8_0 = registerIndexVersion(7_08_00_99, Version.LUCENE_8_5_1, "81d7d459-f386-4c20-8235-f8fce8af7f0e");
    public static final IndexVersion V_7_8_1 = registerIndexVersion(7_08_01_99, Version.LUCENE_8_5_1, "a1b015bc-d020-453f-85a6-9413e169304a");
    public static final IndexVersion V_7_9_0 = registerIndexVersion(7_09_00_99, Version.LUCENE_8_6_0, "0fa951a2-43ce-4f76-91bf-066c1ecf8a93");
    public static final IndexVersion V_7_9_1 = registerIndexVersion(7_09_01_99, Version.LUCENE_8_6_2, "5fc4aabc-080e-4840-af4f-a724deba98b1");
    public static final IndexVersion V_7_9_2 = registerIndexVersion(7_09_02_99, Version.LUCENE_8_6_2, "ef824617-332e-4b63-969e-ebb73a868462");
    public static final IndexVersion V_7_9_3 = registerIndexVersion(7_09_03_99, Version.LUCENE_8_6_2, "499c810a-0f37-4dfd-92ad-55e4936f3578");
    public static final IndexVersion V_7_10_0 = registerIndexVersion(7_10_00_99, Version.LUCENE_8_7_0, "92ccd91c-0251-4263-8873-9f1abfac3c10");
    public static final IndexVersion V_7_10_1 = registerIndexVersion(7_10_01_99, Version.LUCENE_8_7_0, "8ce37467-964f-43eb-ad2d-a51a50116868");
    public static final IndexVersion V_7_10_2 = registerIndexVersion(7_10_02_99, Version.LUCENE_8_7_0, "cb277ccb-3081-4238-be2c-c3167316a435");
    public static final IndexVersion V_7_11_0 = registerIndexVersion(7_11_00_99, Version.LUCENE_8_7_0, "e6d65f96-26d5-4669-ac5a-2964b9b1699f");
    public static final IndexVersion V_7_11_1 = registerIndexVersion(7_11_01_99, Version.LUCENE_8_7_0, "e3655b78-14f7-4432-aa28-34cd1ef2d229");
    public static final IndexVersion V_7_11_2 = registerIndexVersion(7_11_02_99, Version.LUCENE_8_7_0, "1ecfd0ee-4868-4384-b2a0-af6ecb01e496");
    public static final IndexVersion V_7_12_0 = registerIndexVersion(7_12_00_99, Version.LUCENE_8_8_0, "39e2989a-a9a4-4f1a-b185-2e6015f74b1c");
    public static final IndexVersion V_7_12_1 = registerIndexVersion(7_12_01_99, Version.LUCENE_8_8_0, "a8307f67-8295-4567-a7eb-2a6e69046282");
    public static final IndexVersion V_7_13_0 = registerIndexVersion(7_13_00_99, Version.LUCENE_8_8_2, "28b21fe0-4d1f-4c04-95cc-74df494ae0cf");
    public static final IndexVersion V_7_13_1 = registerIndexVersion(7_13_01_99, Version.LUCENE_8_8_2, "4952d7a7-d9f5-443b-b362-8c5ebdc57f81");
    public static final IndexVersion V_7_13_2 = registerIndexVersion(7_13_02_99, Version.LUCENE_8_8_2, "d77c4245-9d26-4da3-aa61-78ab34c3c792");
    public static final IndexVersion V_7_13_3 = registerIndexVersion(7_13_03_99, Version.LUCENE_8_8_2, "a263a47e-4075-4c68-8a42-15a37455c30f");
    public static final IndexVersion V_7_13_4 = registerIndexVersion(7_13_04_99, Version.LUCENE_8_8_2, "d17644c8-3144-495d-8f6c-42cd36698e98");
    public static final IndexVersion V_7_14_0 = registerIndexVersion(7_14_00_99, Version.LUCENE_8_9_0, "b45bb223-bb73-4379-a46f-7dc74d38aaca");
    public static final IndexVersion V_7_14_1 = registerIndexVersion(7_14_01_99, Version.LUCENE_8_9_0, "ee4a6d62-9e05-490b-93dd-b316f9a62d71");
    public static final IndexVersion V_7_14_2 = registerIndexVersion(7_14_02_99, Version.LUCENE_8_9_0, "285d3293-2896-431c-97dd-180890840947");
    public static final IndexVersion V_7_15_0 = registerIndexVersion(7_15_00_99, Version.LUCENE_8_9_0, "ab666b02-b866-4b64-9ba3-d511e86c55b5");
    public static final IndexVersion V_7_15_1 = registerIndexVersion(7_15_01_99, Version.LUCENE_8_9_0, "5643957d-9b68-414a-8917-ea75cf549f67");
    public static final IndexVersion V_7_15_2 = registerIndexVersion(7_15_02_99, Version.LUCENE_8_9_0, "1a618039-d665-47ce-b6ca-886e88c64051");
    public static final IndexVersion V_7_16_0 = registerIndexVersion(7_16_00_99, Version.LUCENE_8_10_1, "a582e900-2d92-474c-9be3-2e08fa88be4b");
    public static final IndexVersion V_7_16_1 = registerIndexVersion(7_16_01_99, Version.LUCENE_8_10_1, "bf666306-9b0d-468b-99dc-f2706dae9c11");
    public static final IndexVersion V_7_16_2 = registerIndexVersion(7_16_02_99, Version.LUCENE_8_10_1, "167c6d69-cae2-4281-8f37-984231620ee9");
    public static final IndexVersion V_7_16_3 = registerIndexVersion(7_16_03_99, Version.LUCENE_8_10_1, "5d25a795-2be6-4663-93dc-10d88efb7e3d");
    public static final IndexVersion V_7_17_0 = registerIndexVersion(7_17_00_99, Version.LUCENE_8_11_1, "18766ab8-4691-40a2-94f1-526f3b71420c");
    public static final IndexVersion V_7_17_1 = registerIndexVersion(7_17_01_99, Version.LUCENE_8_11_1, "8ad49144-4a1c-4322-b33d-614a569fba9b");
    public static final IndexVersion V_7_17_2 = registerIndexVersion(7_17_02_99, Version.LUCENE_8_11_1, "50033cde-c905-4923-83d6-8139f3f110e1");
    public static final IndexVersion V_7_17_3 = registerIndexVersion(7_17_03_99, Version.LUCENE_8_11_1, "460b91d1-4f3d-4f56-8dca-8d9e15f5b862");
    public static final IndexVersion V_7_17_4 = registerIndexVersion(7_17_04_99, Version.LUCENE_8_11_1, "26e40d6f-ac7c-43a3-bd0c-1ec6c3093f66");
    public static final IndexVersion V_7_17_5 = registerIndexVersion(7_17_05_99, Version.LUCENE_8_11_1, "d80bc13c-7139-4ff9-979d-42701d480e33");
    public static final IndexVersion V_7_17_6 = registerIndexVersion(7_17_06_99, Version.LUCENE_8_11_1, "0b47328e-341a-4f97-927d-c49f5050778d");
    public static final IndexVersion V_7_17_7 = registerIndexVersion(7_17_07_99, Version.LUCENE_8_11_1, "b672ff6b-8078-4f6e-b426-6fcf7f8687b4");
    public static final IndexVersion V_7_17_8 = registerIndexVersion(7_17_08_99, Version.LUCENE_8_11_1, "0faffa1b-5fb3-4439-9367-f154fb25395f");
    public static final IndexVersion V_7_17_9 = registerIndexVersion(7_17_09_99, Version.LUCENE_8_11_1, "8044989f-77ef-4d6d-9dd8-1bdd805cef74");
    public static final IndexVersion V_7_17_10 = registerIndexVersion(7_17_10_99, Version.LUCENE_8_11_1, "66b743fb-8be6-443f-8920-d8c5ed561857");
    public static final IndexVersion V_7_17_11 = registerIndexVersion(7_17_11_99, Version.LUCENE_8_11_1, "f1935acc-1af9-44b0-97e9-67112d333753");
    public static final IndexVersion V_8_0_0 = registerIndexVersion(8_00_00_99, Version.LUCENE_9_0_0, "ff18a13c-1fa7-4cf7-a3b1-4fdcd9461d5b");
    public static final IndexVersion V_8_0_1 = registerIndexVersion(8_00_01_99, Version.LUCENE_9_0_0, "4bd5650f-3eff-418f-a7a6-ad46b2a9c941");
    public static final IndexVersion V_8_1_0 = registerIndexVersion(8_01_00_99, Version.LUCENE_9_0_0, "b4742461-ee43-4fd0-a260-29f8388b82ec");
    public static final IndexVersion V_8_1_1 = registerIndexVersion(8_01_01_99, Version.LUCENE_9_0_0, "3883e088-9a1c-4494-8d71-768820485f33");
    public static final IndexVersion V_8_1_2 = registerIndexVersion(8_01_02_99, Version.LUCENE_9_0_0, "859a745a-81d3-463e-af58-615179a22d4f");
    public static final IndexVersion V_8_1_3 = registerIndexVersion(8_01_03_99, Version.LUCENE_9_0_0, "27a49f3f-d3ac-4b0e-8bba-1be24daf4a56");
    public static final IndexVersion V_8_2_0 = registerIndexVersion(8_02_00_99, Version.LUCENE_9_1_0, "af0ed990-2f32-42b5-aaf3-59d21a3dca7a");
    public static final IndexVersion V_8_2_1 = registerIndexVersion(8_02_01_99, Version.LUCENE_9_1_0, "6e2a3812-062a-4d08-8c35-ddc930e8e246");
    public static final IndexVersion V_8_2_2 = registerIndexVersion(8_02_02_99, Version.LUCENE_9_1_0, "93d1434c-3956-408b-8204-93be8ab78856");
    public static final IndexVersion V_8_2_3 = registerIndexVersion(8_02_03_99, Version.LUCENE_9_1_0, "026f6904-2a04-4476-8097-02a75e37e0f7");
    public static final IndexVersion V_8_3_0 = registerIndexVersion(8_03_00_99, Version.LUCENE_9_2_0, "eca8e8a3-0724-4247-a58d-e4eafcec4b3f");
    public static final IndexVersion V_8_3_1 = registerIndexVersion(8_03_01_99, Version.LUCENE_9_2_0, "dac08798-c0b5-46c9-bf27-d82c617ce41f");
    public static final IndexVersion V_8_3_2 = registerIndexVersion(8_03_02_99, Version.LUCENE_9_2_0, "2a0c5fb9-e8a5-4788-89f8-f5723bd68cee");
    public static final IndexVersion V_8_3_3 = registerIndexVersion(8_03_03_99, Version.LUCENE_9_2_0, "440a5f5c-767a-49f7-8593-dc7627b30397");
    public static final IndexVersion V_8_4_0 = registerIndexVersion(8_04_00_99, Version.LUCENE_9_3_0, "d27324da-b36c-452a-93a8-9b69a6c302a1");
    public static final IndexVersion V_8_4_1 = registerIndexVersion(8_04_01_99, Version.LUCENE_9_3_0, "44108ecd-839b-423e-9ef1-9d457f244fff");
    public static final IndexVersion V_8_4_2 = registerIndexVersion(8_04_02_99, Version.LUCENE_9_3_0, "9c20ed39-8c32-4cf0-9f06-42735cbf604e");
    public static final IndexVersion V_8_4_3 = registerIndexVersion(8_04_03_99, Version.LUCENE_9_3_0, "e7d17607-47c0-4662-b308-beeb9a8ec552");
    public static final IndexVersion V_8_5_0 = registerIndexVersion(8_05_00_99, Version.LUCENE_9_4_1, "c5284b51-7fee-4f34-a837-241bb57a7aa6");
    public static final IndexVersion V_8_5_1 = registerIndexVersion(8_05_01_99, Version.LUCENE_9_4_1, "b23a983c-9630-4a2b-8352-0f52b55ff87e");
    public static final IndexVersion V_8_5_2 = registerIndexVersion(8_05_02_99, Version.LUCENE_9_4_1, "cfc80b6f-cb5c-4a4c-b3af-5fa1000508a8");
    public static final IndexVersion V_8_5_3 = registerIndexVersion(8_05_03_99, Version.LUCENE_9_4_2, "f8ac8061-1b17-4cab-b2f6-94df31f7552e");
    public static final IndexVersion V_8_6_0 = registerIndexVersion(8_06_00_99, Version.LUCENE_9_4_2, "5e78c76c-74aa-464e-9383-89bdffb74db9");
    public static final IndexVersion V_8_6_1 = registerIndexVersion(8_06_01_99, Version.LUCENE_9_4_2, "8dc502be-ef27-43b3-a27b-1cb925cbef7d");
    public static final IndexVersion V_8_6_2 = registerIndexVersion(8_06_02_99, Version.LUCENE_9_4_2, "e1e73b88-d188-4d82-b5e1-dee261418783");
    public static final IndexVersion V_8_7_0 = registerIndexVersion(8_07_00_99, Version.LUCENE_9_5_0, "f9227941-d6f4-462b-957f-2bcd36c28382");
    public static final IndexVersion V_8_7_1 = registerIndexVersion(8_07_01_99, Version.LUCENE_9_5_0, "758780b8-4b0c-44c6-af5d-fdac10b6093a");
    public static final IndexVersion V_8_8_0 = registerIndexVersion(8_08_00_99, Version.LUCENE_9_6_0, "d6ffc8d7-f6bd-469b-8495-01688c310000");
    public static final IndexVersion V_8_8_1 = registerIndexVersion(8_08_01_99, Version.LUCENE_9_6_0, "a613499e-ec1a-4b0b-81d3-a766aff3c27c");
    public static final IndexVersion V_8_8_2 = registerIndexVersion(8_08_02_99, Version.LUCENE_9_6_0, "9db9d888-6be8-4a58-825c-f423fd8c6b00");
    public static final IndexVersion V_8_9_0 = registerIndexVersion(8_09_00_99, Version.LUCENE_9_7_0, "32f6dbab-cc24-4f5b-87b5-015a848480d9");
    public static final IndexVersion V_8_10_0 = registerIndexVersion(8_10_00_99, Version.LUCENE_9_7_0, "2e107286-12ad-4c51-9a6f-f8943663b6e7");
    /*
     * READ THE JAVADOC ABOVE BEFORE ADDING NEW INDEX VERSIONS
     * Detached index versions added below here.
     */

    /**
     * Reference to the most recent index version.
     * This should be the index version with the highest id.
     */
    public static final IndexVersion CURRENT = V_8_10_0;

    /**
     * Reference to the earliest compatible index version to this version of the codebase.
     * This should be the index version used by the first release of the previous major version.
     */
    public static final IndexVersion MINIMUM_COMPATIBLE = V_7_0_0;

    static {
        // see comment on IDS field
        // now we're registered the index versions, we can clear the map
        IDS = null;
    }

    static NavigableMap<Integer, IndexVersion> getAllVersionIds(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        NavigableMap<Integer, IndexVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");

        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(IndexVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }

                IndexVersion version;
                try {
                    version = (IndexVersion) declaredField.get(null);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
                builder.put(version.id, version);

                if (Assertions.ENABLED) {
                    // check the version number is unique
                    var sameVersionNumber = versionIdFields.put(version.id, fieldName);
                    assert sameVersionNumber == null
                        : "Versions ["
                        + sameVersionNumber
                        + "] and ["
                        + fieldName
                        + "] have the same version number ["
                        + version.id
                        + "]. Each IndexVersion should have a different version number";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, IndexVersion> VERSION_IDS;

    static {
        VERSION_IDS = getAllVersionIds(IndexVersion.class);

        assert CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST)
            : "IndexVersion must be upgraded to ["
                + org.apache.lucene.util.Version.LATEST
                + "] is still set to ["
                + CURRENT.luceneVersion
                + "]";
    }

    static Collection<IndexVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    public static IndexVersion fromId(int id) {
        IndexVersion known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }

        // this is a version we don't otherwise know about
        // We need to guess the lucene version.
        // Our best guess is to use the same lucene version as the previous
        // version in the list, assuming that it didn't change.
        // if it's older than any known version use the previous major to the oldest known lucene version
        var prev = VERSION_IDS.floorEntry(id);
        Version luceneVersion = prev != null
            ? prev.getValue().luceneVersion
            : Version.fromBits(VERSION_IDS.firstEntry().getValue().luceneVersion.major - 1, 0, 0);

        return new IndexVersion(id, luceneVersion);
    }

    @Deprecated(forRemoval = true)
    public org.elasticsearch.Version toVersion() {
        return org.elasticsearch.Version.fromId(id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     */
    public static IndexVersion min(IndexVersion version1, IndexVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     */
    public static IndexVersion max(IndexVersion version1, IndexVersion version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    public boolean after(IndexVersion version) {
        return version.id < id;
    }

    public boolean onOrAfter(IndexVersion version) {
        return version.id <= id;
    }

    public boolean before(IndexVersion version) {
        return version.id > id;
    }

    public boolean onOrBefore(IndexVersion version) {
        return version.id >= id;
    }

    public boolean between(IndexVersion lowerInclusive, IndexVersion upperExclusive) {
        if (upperExclusive.onOrBefore(lowerInclusive)) throw new IllegalArgumentException();
        return onOrAfter(lowerInclusive) && before(upperExclusive);
    }

    public boolean isLegacyIndexVersion() {
        return before(MINIMUM_COMPATIBLE);
    }

    @Override
    public int compareTo(IndexVersion other) {
        return Integer.compare(this.id, other.id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexVersion version = (IndexVersion) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
