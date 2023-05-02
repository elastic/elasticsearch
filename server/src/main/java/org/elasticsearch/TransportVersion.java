/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Assertions;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the version of the wire protocol used to communicate between ES nodes.
 * <p>
 * Prior to 8.8.0, the release {@link Version} was used everywhere. This class separates the wire protocol version
 * from the release version.
 * <p>
 * Each transport version constant has an id number, which for versions prior to 8.9.0 is the same as the release version
 * for backwards compatibility. In 8.9.0 this is changed to an incrementing number, disconnected from the release version.
 * <p>
 * The creation of each version constant has a unique id string. This is not actually used in the binary protocol, but is there to ensure
 * each protocol version is only added to the source file once. This string needs to be unique (normally a UUID,
 * but can be any other unique nonempty string).
 * If two concurrent PRs add the same transport version, the different unique ids cause a git conflict, ensuring the second PR to be merged
 * must be updated with the next free version first. Without the unique id string, git will happily merge the two versions together,
 * resulting in the same transport version being used across multiple commits,
 * causing problems when you try to upgrade between those two merged commits.
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link #MINIMUM_COMPATIBLE} field. Previously, this was dynamically calculated
 * from the major/minor versions of {@link Version}, but {@code TransportVersion} does not have separate major/minor version numbers.
 * So the minimum compatible version is hard-coded as the transport version used by the highest minor release of the previous major version.
 * {@link #MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <p>
 * The earliest CCS compatible version is hardcoded at {@link #MINIMUM_CCS_VERSION}, as the transport version used by the
 * previous minor release. This should be updated appropriately whenever a minor release happens.
 * <h2>Adding a new version</h2>
 * A new transport version should be added <em>every time</em> a change is made to the serialization protocol of one or more classes.
 * Each transport version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new transport version, add a new constant at the bottom of the list that is one greater than the current highest version,
 * ensure it has a unique id, and update the {@link #CURRENT} constant to point to the new version.
 * <h2>Reverting a transport version</h2>
 * If you revert a commit with a transport version change, you <em>must</em> ensure there is a <em>new</em> transport version
 * representing the reverted change. <em>Do not</em> let the transport version go backwards, it must <em>always</em> be incremented.
 */
public class TransportVersion implements Comparable<TransportVersion> {
    public static final TransportVersion ZERO = new TransportVersion(0);

    public static final TransportVersion V_7_0_0;
    public static final TransportVersion V_7_0_1;
    public static final TransportVersion V_7_1_0;
    public static final TransportVersion V_7_1_1;
    public static final TransportVersion V_7_2_0;
    public static final TransportVersion V_7_2_1;
    public static final TransportVersion V_7_3_0;
    public static final TransportVersion V_7_3_1;
    public static final TransportVersion V_7_3_2;
    public static final TransportVersion V_7_4_0;
    public static final TransportVersion V_7_4_1;
    public static final TransportVersion V_7_4_2;
    public static final TransportVersion V_7_5_0;
    public static final TransportVersion V_7_5_1;
    public static final TransportVersion V_7_5_2;
    public static final TransportVersion V_7_6_0;
    public static final TransportVersion V_7_6_1;
    public static final TransportVersion V_7_6_2;
    public static final TransportVersion V_7_7_0;
    public static final TransportVersion V_7_7_1;
    public static final TransportVersion V_7_8_0;
    public static final TransportVersion V_7_8_1;
    public static final TransportVersion V_7_9_0;
    public static final TransportVersion V_7_9_1;
    public static final TransportVersion V_7_9_2;
    public static final TransportVersion V_7_9_3;
    public static final TransportVersion V_7_10_0;
    public static final TransportVersion V_7_10_1;
    public static final TransportVersion V_7_10_2;
    public static final TransportVersion V_7_11_0;
    public static final TransportVersion V_7_11_1;
    public static final TransportVersion V_7_11_2;
    public static final TransportVersion V_7_12_0;
    public static final TransportVersion V_7_12_1;
    public static final TransportVersion V_7_13_0;
    public static final TransportVersion V_7_13_1;
    public static final TransportVersion V_7_13_2;
    public static final TransportVersion V_7_13_3;
    public static final TransportVersion V_7_13_4;
    public static final TransportVersion V_7_14_0;
    public static final TransportVersion V_7_14_1;
    public static final TransportVersion V_7_14_2;
    public static final TransportVersion V_7_15_0;
    public static final TransportVersion V_7_15_1;
    public static final TransportVersion V_7_15_2;
    public static final TransportVersion V_7_16_0;
    public static final TransportVersion V_7_16_1;
    public static final TransportVersion V_7_16_2;
    public static final TransportVersion V_7_16_3;
    public static final TransportVersion V_7_17_0;
    public static final TransportVersion V_7_17_1;
    public static final TransportVersion V_7_17_2;
    public static final TransportVersion V_7_17_3;
    public static final TransportVersion V_7_17_4;
    public static final TransportVersion V_7_17_5;
    public static final TransportVersion V_7_17_6;
    public static final TransportVersion V_7_17_7;
    public static final TransportVersion V_7_17_8;
    public static final TransportVersion V_7_17_9;
    public static final TransportVersion V_7_17_10;
    public static final TransportVersion V_8_0_0;
    public static final TransportVersion V_8_0_1;
    public static final TransportVersion V_8_1_0;
    public static final TransportVersion V_8_1_1;
    public static final TransportVersion V_8_1_2;
    public static final TransportVersion V_8_1_3;
    public static final TransportVersion V_8_2_0;
    public static final TransportVersion V_8_2_1;
    public static final TransportVersion V_8_2_2;
    public static final TransportVersion V_8_2_3;
    public static final TransportVersion V_8_3_0;
    public static final TransportVersion V_8_3_1;
    public static final TransportVersion V_8_3_2;
    public static final TransportVersion V_8_3_3;
    public static final TransportVersion V_8_4_0;
    public static final TransportVersion V_8_4_1;
    public static final TransportVersion V_8_4_2;
    public static final TransportVersion V_8_4_3;
    public static final TransportVersion V_8_5_0;
    public static final TransportVersion V_8_5_1;
    public static final TransportVersion V_8_5_2;
    public static final TransportVersion V_8_5_3;
    public static final TransportVersion V_8_5_4;
    public static final TransportVersion V_8_6_0;
    public static final TransportVersion V_8_6_1;
    public static final TransportVersion V_8_6_2;
    public static final TransportVersion V_8_7_0;
    public static final TransportVersion V_8_7_1;
    public static final TransportVersion V_8_8_0;
    public static final TransportVersion V_8_9_0;

    /*
     * READ THE JAVADOC ABOVE BEFORE ADDING NEW TRANSPORT VERSIONS
     * Detached transport versions added below here.
     */

    static {
        try {
            Map<Integer, String> IdMap = new HashMap<>();
            var transportVersionOf = factoryHandle(IdMap);

            V_7_0_0 = (TransportVersion) transportVersionOf.invokeExact(7_00_00_99, "7505fd05-d982-43ce-a63f-ff4c6c8bdeec");
            V_7_0_1 = (TransportVersion) transportVersionOf.invokeExact(7_00_01_99, "ae772780-e6f9-46a1-b0a0-20ed0cae37f7");
            V_7_1_0 = (TransportVersion) transportVersionOf.invokeExact(7_01_00_99, "fd09007c-1c54-450a-af99-9f941e1a53c2");
            V_7_1_1 = (TransportVersion) transportVersionOf.invokeExact(7_01_01_99, "f7ddb16c-3495-42ef-8d54-1461570ca68c");
            V_7_2_0 = (TransportVersion) transportVersionOf.invokeExact(7_02_00_99, "b74dbc52-e727-472c-af21-2156482e8796");
            V_7_2_1 = (TransportVersion) transportVersionOf.invokeExact(7_02_01_99, "a3217b94-f436-4aab-a020-162c83ba18f2");
            V_7_3_0 = (TransportVersion) transportVersionOf.invokeExact(7_03_00_99, "4f04e4c9-c5aa-49e4-8b99-abeb4e284a5a");
            V_7_3_1 = (TransportVersion) transportVersionOf.invokeExact(7_03_01_99, "532b9bc9-e11f-48a2-b997-67ca68ffb354");
            V_7_3_2 = (TransportVersion) transportVersionOf.invokeExact(7_03_02_99, "60da3953-8415-4d4f-a18d-853c3e68ebd6");
            V_7_4_0 = (TransportVersion) transportVersionOf.invokeExact(7_04_00_99, "ec7e58aa-55b4-4064-a9dd-fd723a2ba7a8");
            V_7_4_1 = (TransportVersion) transportVersionOf.invokeExact(7_04_01_99, "a316c26d-8e6a-4608-b1ec-062331552b98");
            V_7_4_2 = (TransportVersion) transportVersionOf.invokeExact(7_04_02_99, "031a77e1-3640-4c8a-80cf-28ded96bab48");
            V_7_5_0 = (TransportVersion) transportVersionOf.invokeExact(7_05_00_99, "cc6e14dc-9dc7-4b74-8e15-1f99a6cfbe03");
            V_7_5_1 = (TransportVersion) transportVersionOf.invokeExact(7_05_01_99, "9d12be44-16dc-44a8-a89a-45c9174ea596");
            V_7_5_2 = (TransportVersion) transportVersionOf.invokeExact(7_05_02_99, "484ed9de-7f5b-4e6b-a79a-0cb5e7570093");
            V_7_6_0 = (TransportVersion) transportVersionOf.invokeExact(7_06_00_99, "4637b8ae-f3df-43ae-a065-ad4c29f3373a");
            V_7_6_1 = (TransportVersion) transportVersionOf.invokeExact(7_06_01_99, "fe5b9f95-a311-4a92-943b-30ec256a331c");
            V_7_6_2 = (TransportVersion) transportVersionOf.invokeExact(7_06_02_99, "5396cb30-d91c-4789-85e8-77efd552c785");
            V_7_7_0 = (TransportVersion) transportVersionOf.invokeExact(7_07_00_99, "7bb73c48-ddb8-4437-b184-30371c35dd4b");
            V_7_7_1 = (TransportVersion) transportVersionOf.invokeExact(7_07_01_99, "85507b0f-0fca-4daf-a80b-451fe75e04a0");
            V_7_8_0 = (TransportVersion) transportVersionOf.invokeExact(7_08_00_99, "c3cc74af-d15e-494b-a907-6ad6dd2f4660");
            V_7_8_1 = (TransportVersion) transportVersionOf.invokeExact(7_08_01_99, "7acb9f6e-32f2-45ce-b87d-ca1f165b8e7a");
            V_7_9_0 = (TransportVersion) transportVersionOf.invokeExact(7_09_00_99, "9388fe76-192a-4053-b51c-d2a7b8eae545");
            V_7_9_1 = (TransportVersion) transportVersionOf.invokeExact(7_09_01_99, "30fa10fc-df6b-4435-bd9e-acdb9ae1b268");
            V_7_9_2 = (TransportVersion) transportVersionOf.invokeExact(7_09_02_99, "b58bb181-cecc-464e-b955-f6c1c1e7b4d0");
            V_7_9_3 = (TransportVersion) transportVersionOf.invokeExact(7_09_03_99, "4406926c-e2b6-4b9a-a72a-1bee8357ad3e");
            V_7_10_0 = (TransportVersion) transportVersionOf.invokeExact(7_10_00_99, "4efca195-38e4-4f74-b877-c26fb2a40733");
            V_7_10_1 = (TransportVersion) transportVersionOf.invokeExact(7_10_01_99, "0070260c-aa0b-4fc2-9c87-5cd5f23b005f");
            V_7_10_2 = (TransportVersion) transportVersionOf.invokeExact(7_10_02_99, "b369e2ed-261c-4b2f-8b42-0f0ba0549f8c");
            V_7_11_0 = (TransportVersion) transportVersionOf.invokeExact(7_11_00_99, "3b43bcbc-1c5e-4cc2-a3b4-8ac8b64239e8");
            V_7_11_1 = (TransportVersion) transportVersionOf.invokeExact(7_11_01_99, "2f75d13c-adde-4762-a46e-def8acce62b7");
            V_7_11_2 = (TransportVersion) transportVersionOf.invokeExact(7_11_02_99, "2c852a4b-236d-4e8b-9373-336c9b52685a");
            V_7_12_0 = (TransportVersion) transportVersionOf.invokeExact(7_12_00_99, "3be9ff6f-2d9f-4fc2-ba91-394dd5ebcf33");
            V_7_12_1 = (TransportVersion) transportVersionOf.invokeExact(7_12_01_99, "ee4fdfac-2039-4b00-b42d-579cbde7120c");
            V_7_13_0 = (TransportVersion) transportVersionOf.invokeExact(7_13_00_99, "e1fe494a-7c66-4571-8f8f-1d7e6d8df1b3");
            V_7_13_1 = (TransportVersion) transportVersionOf.invokeExact(7_13_01_99, "66bc8d82-36da-4d54-b22d-aca691dc3d70");
            V_7_13_2 = (TransportVersion) transportVersionOf.invokeExact(7_13_02_99, "2a6fc74c-4c44-4264-a619-37437cd2c5a0");
            V_7_13_3 = (TransportVersion) transportVersionOf.invokeExact(7_13_03_99, "a31592f5-f8d2-490c-a02e-da9501823d8d");
            V_7_13_4 = (TransportVersion) transportVersionOf.invokeExact(7_13_04_99, "3143240d-1831-4186-8a19-963336c4cea0");
            V_7_14_0 = (TransportVersion) transportVersionOf.invokeExact(7_14_00_99, "8cf0954c-b085-467f-b20b-3cb4b2e69e3e");
            V_7_14_1 = (TransportVersion) transportVersionOf.invokeExact(7_14_01_99, "3dbb62c3-cf73-4c76-8d5a-4ca70afe2c70");
            V_7_14_2 = (TransportVersion) transportVersionOf.invokeExact(7_14_02_99, "7943ae20-df60-45e5-97ba-82fc0dfc8b89");
            V_7_15_0 = (TransportVersion) transportVersionOf.invokeExact(7_15_00_99, "2273ac0e-00bb-4024-9e2e-ab78981623c6");
            V_7_15_1 = (TransportVersion) transportVersionOf.invokeExact(7_15_01_99, "a8c3503d-3452-45cf-b385-e855e16547fe");
            V_7_15_2 = (TransportVersion) transportVersionOf.invokeExact(7_15_02_99, "fbb8ad69-02e2-4c90-b2e4-23947107f8b4");
            V_7_16_0 = (TransportVersion) transportVersionOf.invokeExact(7_16_00_99, "59abadd2-25db-4547-a991-c92306a3934e");
            V_7_16_1 = (TransportVersion) transportVersionOf.invokeExact(7_16_01_99, "4ace6b6b-8bba-427f-8755-9e3b40092138");
            V_7_16_2 = (TransportVersion) transportVersionOf.invokeExact(7_16_02_99, "785567b9-b320-48ef-b538-1753228904cd");
            V_7_16_3 = (TransportVersion) transportVersionOf.invokeExact(7_16_03_99, "facf5ae7-3d4e-479c-9142-72529b784e30");
            V_7_17_0 = (TransportVersion) transportVersionOf.invokeExact(7_17_00_99, "322efe93-4c73-4e15-9274-bb76836c8fa8");
            V_7_17_1 = (TransportVersion) transportVersionOf.invokeExact(7_17_01_99, "51c72842-7974-4669-ad25-bf13ba307307");
            V_7_17_2 = (TransportVersion) transportVersionOf.invokeExact(7_17_02_99, "82bea8d0-bfea-47c2-b7d3-217d8feb67e3");
            V_7_17_3 = (TransportVersion) transportVersionOf.invokeExact(7_17_03_99, "a909c2f4-5cb8-46bf-af0f-cd18d1b7e9d2");
            V_7_17_4 = (TransportVersion) transportVersionOf.invokeExact(7_17_04_99, "5076e164-18a4-4373-8be7-15f1843c46db");
            V_7_17_5 = (TransportVersion) transportVersionOf.invokeExact(7_17_05_99, "da7e3509-7f61-4dd2-8d23-a61f628a62f6");
            V_7_17_6 = (TransportVersion) transportVersionOf.invokeExact(7_17_06_99, "a47ecf02-e457-474f-887d-ee15a7ebd969");
            V_7_17_7 = (TransportVersion) transportVersionOf.invokeExact(7_17_07_99, "108ba576-bb28-42f4-bcbf-845a0ce52560");
            V_7_17_8 = (TransportVersion) transportVersionOf.invokeExact(7_17_08_99, "82a3e70d-cf0e-4efb-ad16-6077ab9fe19f");
            V_7_17_9 = (TransportVersion) transportVersionOf.invokeExact(7_17_09_99, "afd50dda-735f-4eae-9309-3218ffec1b2d");
            V_7_17_10 = (TransportVersion) transportVersionOf.invokeExact(7_17_10_99, "18ae7108-6f7a-4205-adbb-cfcd6aa6ccc6");
            V_8_0_0 = (TransportVersion) transportVersionOf.invokeExact(8_00_00_99, "c7d2372c-9f01-4a79-8b11-227d862dfe4f");
            V_8_0_1 = (TransportVersion) transportVersionOf.invokeExact(8_00_01_99, "56e044c3-37e5-4f7e-bd38-f493927354ac");
            V_8_1_0 = (TransportVersion) transportVersionOf.invokeExact(8_01_00_99, "3dc49dce-9cef-492a-ac8d-3cc79f6b4280");
            V_8_1_1 = (TransportVersion) transportVersionOf.invokeExact(8_01_01_99, "40cf32e5-17b0-4187-9de1-022cdea69db9");
            V_8_1_2 = (TransportVersion) transportVersionOf.invokeExact(8_01_02_99, "54aa6394-08f3-4db7-b82e-314ae4b5b562");
            V_8_1_3 = (TransportVersion) transportVersionOf.invokeExact(8_01_03_99, "9772b54b-1e14-485f-92e8-8847b3a3d569");
            V_8_2_0 = (TransportVersion) transportVersionOf.invokeExact(8_02_00_99, "8ce6d555-202e-47db-ab7d-ade9dda1b7e8");
            V_8_2_1 = (TransportVersion) transportVersionOf.invokeExact(8_02_01_99, "ffbb67e8-cc33-4b02-a995-b461d9ee36c8");
            V_8_2_2 = (TransportVersion) transportVersionOf.invokeExact(8_02_02_99, "2499ee77-187d-4e10-8366-8e60d5f03676");
            V_8_2_3 = (TransportVersion) transportVersionOf.invokeExact(8_02_03_99, "046aae43-3090-4ece-8c27-8d489f097548");
            V_8_3_0 = (TransportVersion) transportVersionOf.invokeExact(8_03_00_99, "559ddb66-d857-4208-bed5-a995ccf478ea");
            V_8_3_1 = (TransportVersion) transportVersionOf.invokeExact(8_03_01_99, "31f9b136-dbbe-4fa1-b811-d6afa2a1b472");
            V_8_3_2 = (TransportVersion) transportVersionOf.invokeExact(8_03_02_99, "f6e9cd4c-2a71-4f9b-80d4-7ba97ebd18b2");
            V_8_3_3 = (TransportVersion) transportVersionOf.invokeExact(8_03_03_99, "a784de3e-533e-4844-8728-c55c6932dd8e");
            V_8_4_0 = (TransportVersion) transportVersionOf.invokeExact(8_04_00_99, "c0d12906-aa5b-45d4-94c7-cbcf4d9818ca");
            V_8_4_1 = (TransportVersion) transportVersionOf.invokeExact(8_04_01_99, "9a915f76-f259-4361-b53d-3f19c7797fd8");
            V_8_4_2 = (TransportVersion) transportVersionOf.invokeExact(8_04_02_99, "87c5b7b2-0f57-4172-8a81-b9f9a0198525");
            V_8_4_3 = (TransportVersion) transportVersionOf.invokeExact(8_04_03_99, "327cb1a0-9b5d-4be9-8033-285c2549f770");
            V_8_5_0 = (TransportVersion) transportVersionOf.invokeExact(8_05_00_99, "be3d7f23-7240-4904-9d7f-e25a0f766eca");
            V_8_5_1 = (TransportVersion) transportVersionOf.invokeExact(8_05_01_99, "d349d202-f01c-4dbb-85dd-947fb4267c99");
            V_8_5_2 = (TransportVersion) transportVersionOf.invokeExact(8_05_02_99, "b68b1331-fd64-44d9-9e71-f6796ec2024c");
            V_8_5_3 = (TransportVersion) transportVersionOf.invokeExact(8_05_03_99, "9ca3c835-e3b7-4622-a08e-d51e42403b06");
            V_8_5_4 = (TransportVersion) transportVersionOf.invokeExact(8_05_04_99, "97ee525c-555d-45ca-83dc-59cd592c8e86");
            V_8_6_0 = (TransportVersion) transportVersionOf.invokeExact(8_06_00_99, "e209c5ed-3488-4415-b561-33492ca3b789");
            V_8_6_1 = (TransportVersion) transportVersionOf.invokeExact(8_06_01_99, "9f113acb-1b21-4fda-bef9-2a3e669b5c7b");
            V_8_6_2 = (TransportVersion) transportVersionOf.invokeExact(8_06_02_99, "5a82fb68-b265-4a06-97c5-53496f823f51");
            V_8_7_0 = (TransportVersion) transportVersionOf.invokeExact(8_07_00_99, "f1ee7a85-4fa6-43f5-8679-33e2b750448b");
            V_8_7_1 = (TransportVersion) transportVersionOf.invokeExact(8_07_01_99, "018de9d8-9e8b-4ac7-8f4b-3a6fbd0487fb");
            V_8_8_0 = (TransportVersion) transportVersionOf.invokeExact(8_08_00_99, "f64fe576-0767-4ec3-984e-3e30b33b6c46");
            V_8_9_0 = (TransportVersion) transportVersionOf.invokeExact(8_09_00_99, "13c1c2cb-d975-461f-ab98-309ebc1c01bc");

            VERSION_IDS = getAllVersionIds(TransportVersion.class, IdMap);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    /**
     * Reference to the most recent transport version.
     * This should be the transport version with the highest id.
     */
    public static final TransportVersion CURRENT = V_8_9_0;

    /**
     * Reference to the earliest compatible transport version to this version of the codebase.
     * This should be the transport version used by the highest minor version of the previous major.
     */
    public static final TransportVersion MINIMUM_COMPATIBLE = V_7_17_0;

    /**
     * Reference to the minimum transport version that can be used with CCS.
     * This should be the transport version used by the previous minor release.
     */
    public static final TransportVersion MINIMUM_CCS_VERSION = V_8_7_0;

    static NavigableMap<Integer, TransportVersion> getAllVersionIds(Class<?> cls, Map<Integer, String> origMap) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        Map<String, String> uniqueIdFields = new HashMap<>();
        NavigableMap<Integer, TransportVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE", "MINIMUM_CCS_VERSION");
        Pattern bwcVersionField = Pattern.compile("^V_(\\d_\\d{1,2}_\\d{1,2})$");
        Pattern transportVersionField = Pattern.compile("^V_(\\d+_\\d{3}_\\d{3})$");

        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(TransportVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }

                // check the field modifiers
                if (declaredField.getModifiers() != (Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL)) {
                    assert false : "Version field [" + fieldName + "] should be public static final";
                    continue;
                }

                TransportVersion version;
                try {
                    version = (TransportVersion) declaredField.get(null);
                } catch (IllegalAccessException e) {
                    // should not happen, checked above
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
                            + "]. Each TransportVersion should have a different version number";

                    // check the name matches the version number
                    try {
                        int fieldNumber;
                        int idNumber = version.id;
                        Matcher matcher = bwcVersionField.matcher(fieldName);
                        if (matcher.matches()) {
                            // match single digits _\d_ or _\d$ to put a 0 in front, but do not actually capture the _ or $
                            fieldNumber = Integer.parseInt(matcher.group(1).replaceAll("_(\\d)(?=_|$)", "_0$1").replace("_", ""));
                            idNumber /= 100;    // remove the extra '99'
                        } else if ((matcher = transportVersionField.matcher(fieldName)).matches()) {
                            fieldNumber = Integer.parseInt(matcher.group(1).replace("_", ""));
                        } else {
                            assert false : "Version [" + fieldName + "] does not have the correct name format";
                            continue;
                        }

                        assert fieldNumber == idNumber : "Version [" + fieldName + "] does not match its version number [" + idNumber + "]";
                    } catch (NumberFormatException e) {
                        assert false : "Version [" + fieldName + "] does not have the correct name format";
                        continue;
                    }

                    // check the id is unique
                    String uniqueId = origMap.get(version.id);
                    var sameUniqueId = uniqueIdFields.put(uniqueId, fieldName);
                    assert sameUniqueId == null
                        : "Versions ["
                            + fieldName
                            + "] and ["
                            + sameUniqueId
                            + "] have the same unique id. Each TransportVersion should have a different unique id";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, TransportVersion> VERSION_IDS;

    static Collection<TransportVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    public static TransportVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static TransportVersion fromId(int id) {
        TransportVersion known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }
        // this is a version we don't otherwise know about - just create a placeholder
        return new TransportVersion(id);
    }

    public static void writeVersion(TransportVersion version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     */
    public static TransportVersion min(TransportVersion version1, TransportVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     */
    public static TransportVersion max(TransportVersion version1, TransportVersion version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    static MethodHandle factoryHandle(Map<Integer, String> map) {
        try {
            var mt = MethodType.methodType(TransportVersion.class, Map.class, int.class, String.class);
            var mh = MethodHandles.lookup().findStatic(TransportVersion.class, "transportVersionOf", mt);
            return mh.bindTo(map);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    private static TransportVersion transportVersionOf(Map<Integer, String> IdMap, int id, String uniqueId) {
        Strings.requireNonEmpty(uniqueId, "Each TransportVersion needs a unique string id");
        checkAndAddId(IdMap, id, uniqueId);
        return new TransportVersion(id);
    }

    /** Add the id/uniqueId to the given map, verifying that the id is not previously present. */
    private static void checkAndAddId(Map<Integer, String> IdMap, int id, String uniqueId) {
        var prevUniqueId = IdMap.put(id, uniqueId);
        if (prevUniqueId != null) {
            throw new IllegalArgumentException(
                "uniqueId ["
                    + uniqueId
                    + "] and ["
                    + prevUniqueId
                    + "] have the same version number ["
                    + id
                    + "]. Each TransportVersion should have a different version number"
            );
        }
    }

    public final int id;

    TransportVersion(int id) {
        this.id = id;
    }

    /**
     * Returns {@code true} if the specified version is compatible with this running version of Elasticsearch.
     */
    public static boolean isCompatible(TransportVersion version) {
        return version.onOrAfter(MINIMUM_COMPATIBLE);
    }

    public boolean after(TransportVersion version) {
        return version.id < id;
    }

    public boolean onOrAfter(TransportVersion version) {
        return version.id <= id;
    }

    public boolean before(TransportVersion version) {
        return version.id > id;
    }

    public boolean onOrBefore(TransportVersion version) {
        return version.id >= id;
    }

    public static TransportVersion fromString(String str) {
        return TransportVersion.fromId(Integer.parseInt(str));
    }

    @Override
    public int compareTo(TransportVersion other) {
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

        TransportVersion version = (TransportVersion) o;

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
