/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.common.hash.MessageDigests;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * Contains license expiration date overrides.
 */
class LicenseOverrides {
    private LicenseOverrides() {}

    // This approach of just having everything in a hardcoded map is based on an assumption that we will need to do this very infrequently.
    // If this assumption proves incorrect, we should consider switching to another approach,
    private static final Map<String, ZonedDateTime> LICENSE_OVERRIDES;

    static {
        // This value is not a "real" license ID, it is used for testing this code
        String TEST_LICENSE_ID_HASH = MessageDigests.toHexString(
            MessageDigests.sha256().digest("12345678-abcd-0000-0000-000000000000".getBytes(StandardCharsets.UTF_8))
        );

        assert TEST_LICENSE_ID_HASH.equals("d180f3dedf21b96eea4021d373ab990cba53eeb6c44832261417e828fcb278f1");

        ZonedDateTime EXPIRED = ZonedDateTime.ofStrict(LocalDateTime.of(1970, 1, 1, 0, 0, 42, 0), ZoneOffset.UTC, ZoneOffset.UTC);
        LICENSE_OVERRIDES = Map.ofEntries(
            Map.entry(TEST_LICENSE_ID_HASH, EXPIRED),
            Map.entry("bef6c45f0b1d964e0509bcd3e3ae74513346fbd39fdc9ead6d06b714979f59f0", EXPIRED),
            Map.entry("87ff1c3f4a318f255da0a38110f3abf1869948a5e77bbf5cd422ba81cdfc1006", EXPIRED),
            Map.entry("98455271599065062222c68e263defe79076451db712e3b72561b0783905a5ec", EXPIRED),
            Map.entry("ea2d863f40baf31e95717dcd49314d1fd926f4cb64ca22c88ba385cc139d7a53", EXPIRED),
            Map.entry("f796951eea28f3858ca119f250088621255a4d70d0adce29d1aeebcb372ac2ae", EXPIRED),
            Map.entry("874a3b4ffd6b105cf805ab0a8c1f1f5cd3e9c00219b34a718e73194e6f5b7703", EXPIRED),
            Map.entry("54a663717357170a30b9ab8fee183d764892dabe28ca908a29db6f7ea0e20c0e", EXPIRED),
            Map.entry("9f00c2921b24c8fcd665ee47360d7088f38aec0cb947b27c681510c1d7753380", EXPIRED),
            Map.entry("03ebc3421aa3251137230ae20acca998e4ec2c50c8985d0b84eb0ae871459b7d", EXPIRED),
            Map.entry("9d8aab0e5c3a879925cd8ccec4a9fc8763f2d8baa9389a347f8c5515a7830d25", EXPIRED),
            Map.entry("2e82a648be7c415455f9ff9ac17255b9ba08efeada26e1f1c2f960f5c941f59f", EXPIRED),
            Map.entry("bc922d6075f378e9ee2fbca316caf8d79c6b48362a5c58ccaf06feca9d59a1c1", EXPIRED),
            Map.entry("5c947456cd04beacb0356e692fde5ea888d2d98cef4e59121913ba09fa29eb29", EXPIRED),
            Map.entry("60fa9ceb6f7bdb9e01cef469177e3df6eb108c17aab68ec3c231b4e9cbcef80b", EXPIRED),
            Map.entry("42e8788dd4adf5e85f79e4fc4adff7012362f46124b1ba14ad1dc868e9db2948", EXPIRED),
            Map.entry("30cf34a01ef9e9d6c52572a3c6aa9abd10ce120c58ce66ba4014c8e6ca375159", EXPIRED),
            Map.entry("1def2cae6a400b8c2eae44117536c0af8b128f58eecbd44f9fe32003501f241e", EXPIRED),
            Map.entry("1f43c471c1cf9a7118829abac814314502ea634f9e8b882b9caaed7a9aa39f87", EXPIRED),
            Map.entry("4d9e769143e9add890bc040406ab21d73b116946854e00a58dc2805ad3717c3c", EXPIRED),
            Map.entry("2b776c2e49052b6006686977f22e851105c173c743e0e410a7a360c5fe16f4b5", EXPIRED),
            Map.entry("5e6258c0be37392ca2bb5048bd93278dc1c424112cc65c048690c9ca246f9631", EXPIRED),
            Map.entry("c54a38ed90304ff731f7d2883ef5d0ba9f64fcc0710e8060358fde813699fa80", EXPIRED),
            Map.entry("dc1a2d1f36ed68ccc18637acf7262f7c807b7f300c70699ad28d35ad6fe74173", EXPIRED),
            Map.entry("ead8fa12d35fc1d33b5c4f1786c33ae55847969f0a3bbda6bc2fa973376a4e95", EXPIRED),
            Map.entry("703efa46a1690c6346ca6d849528e76c6fbbc7bffd33a8f7cc7cb09e58fb625c", EXPIRED),
            Map.entry("64063f0aad016fd927a4c88176c393546aa4d63ea284e58253b7a37fc1aaad90", EXPIRED),
            Map.entry("f7ecf7c89d35621697610b11955274568e49f0001992563c521aaedc3c31cbf7", EXPIRED),
            Map.entry("697fcb0dfcdf3cc7b7d3901359a41963b1c55f5d1ca08e37a5a3639034ec46f1", EXPIRED),
            Map.entry("422c4196c0fc3bf55359363c8c6a3a42125c48221d4852fbb1cff354230a5e8c", EXPIRED),
            Map.entry("649daeaba6361606ba782b3a758cdc65745f7e631bb70284208f301f89a24d01", EXPIRED),
            Map.entry("f3346f4a6871a674f8cf904f6c73cbc3824de896bd7202ad451fb418384be88b", EXPIRED),
            Map.entry("747b5c65e7ef564dbc54912ab39b959311545d80888b43ce877177eac707a598", EXPIRED),
            Map.entry("febb69a5eb3add9f21296c24de09b16cb71f124dddc9f87449e307e8eb581201", EXPIRED),
            Map.entry("f948100e64218aef50459fcf255ce6cbccb6b35b5ea1328fdd91b0751915d369", EXPIRED),
            Map.entry("99b69ab1c1f53d0f3c77095b8a12a3a4c762be3b48b5065ea2067bc54f57335a", EXPIRED),
            Map.entry("9b10eca421c864d4e326c0717959a6ab1ad65bfe5e294e0fa4339c0fa6acea24", EXPIRED),
            Map.entry("acbba3189de854952c116493d4d8d9f5f2ba6a06098229f887d9f457eb524e28", EXPIRED),
            Map.entry("1aeff63eb00deacf4542725237c2e252157d690c709bcbb2fafabfcb3ad9c710", EXPIRED),
            Map.entry("10bf92794a52eddf75a8688c96b37fb34c07809613f7c9dfa5ebe5a8c4f44a41", EXPIRED),
            Map.entry("bc799fc331675bf1fdfc850f00a807966a852fc36eaa20d0cc00622917550cce", EXPIRED),
            Map.entry("100e336565e95a588b626f5be4bec03070da98e4a4ed03003ba87b54cd23b903", EXPIRED),
            Map.entry("6692e5ff7fad261304d607464d20011f711e9c279f8affab643b9623bf8a427b", EXPIRED),
            Map.entry("82838a55332eb4e0558b853c677b71973b95dc556208499e44346b84e3b344ba", EXPIRED),
            Map.entry("0c1ad85b0061ccce9e5d39ce8e4846661576759cd1a439d3d6d43db923ec5571", EXPIRED),
            Map.entry("dbda516df69c5eb236be5c984ea4d1a12beb24348b274963cc13a2df96fbef22", EXPIRED),
            Map.entry("2d86c7aa9fe12b7278c55cf40fa34a316b81d33209b7689b39f9098b7d590dd9", EXPIRED),
            Map.entry("e5946d5d667141c4148eb4547227c90964364eade6f11866572309a13962733b", EXPIRED),
            Map.entry("1432aaa6dc9fa5f749396ecf93fc9458d5d19046d5989b75a93f27d7575a296d", EXPIRED),
            Map.entry("848b0a30e61b25dc992c137fbc90d7f3c56a9c9442b5e9f3ceee37e0fec9aded", EXPIRED),
            Map.entry("52d459cc3f48638b5ee817bb04bbfa3eabf9cac2cc0c1fc10f32d2ab4cc17538", EXPIRED),
            Map.entry("518908aa6fbc60a1a3a5527609c25b772c459690ee1c55a0cb6412bfe9150783", EXPIRED),
            Map.entry("3bfec3fc8b4bd2ba2e2fd5015aa395869ece98b6a431e86d315198e0f8d6cec3", EXPIRED),
            Map.entry("1904e0f89e595b6993228a661ca7b599a20ece0e06046738111070f9b5501af6", EXPIRED),
            Map.entry("2b2f8afb654c099872ff9b344516828d7b9caa6f2fab2ca6d79fc4a81780c1ef", EXPIRED),
            Map.entry("18731afafd4d78de951984f8a367d5785c6d98291af417262999e86ace6972aa", EXPIRED),
            Map.entry("73905cb5b93aaa1c4338a3d69882f83ab8355a825898bc57c78034f3cdcd44b1", EXPIRED),
            Map.entry("374fa63988e30b8dd604f42ee90d2ea5c6d80ee564390566457c57b937fd02ad", EXPIRED),
            Map.entry("eeb4fce1e464699a1b0a7a50721b1f4857c2ff3ee7a4db9f8493b00656c3ddc3", EXPIRED),
            Map.entry("f6f055c57f373bbf05a3ad995d8aaafa778c8927a6792a683a062ae875713f3d", EXPIRED),
            Map.entry("9e111464000dcb92246c394aa06fd7d75f3574cbabba140234b01f9a1af927cf", EXPIRED),
            Map.entry("99ee590949f7bbaf39224f45908a5062373993d2d11f20884ff762f19bfb131a", EXPIRED),
            Map.entry("773239ee40f98ee4d56a6e1c16b7428b6a229f828fe0af2c4b66420bcc4bb11a", EXPIRED),
            Map.entry("6a3832e101cdd467c508a004bcb3836eadad465e8c34b18123e937aa21899fb8", EXPIRED),
            Map.entry("d182e681219884272ef10e08049fe00133bcec0a9a8ac343c41432186757263e", EXPIRED),
            Map.entry("f7c72a3e5a1dbd9055775cddb097fcd5322a09ce453920fa85df27cc716c1623", EXPIRED),
            Map.entry("2e75c00fa52d2ed47f087a0d3eecc487cd35554dad872d4659bbb4328ef82e86", EXPIRED),
            Map.entry("d6f1214c4f84b0ceb7bc22f45cc42de538b7da3318028d9a04ea374017cff155", EXPIRED),
            Map.entry("d0631181360040f56262024aed3eec43e13cdb2c97bd35ce0ba7455879f9c927", EXPIRED),
            Map.entry("06eaf36e826fbd0072882f2c96eb3c10ae967120d56a386e29963f42fe3cf54e", EXPIRED),
            Map.entry("631003710c2c53a5ee2d401c5aadc1362904ab75743884bcadc3b2047330dc75", EXPIRED),
            Map.entry("d5a45609971ded61d6c2ee50bc1d07ae1ae54bb49fd3515d4d359c1a23dcc616", EXPIRED),
            Map.entry("569dd8f18cabd696bfa573ac6fe348327c3a28ce5a26a3e6cfe25411b084cd8e", EXPIRED),
            Map.entry("b79390bdeddddde4bcf2cade20bf97d79e7f6b7139b694f43a25bd9efc53d4ea", EXPIRED),
            Map.entry("bb810cebdb9baa770f47f04d0331e3037956d4cdcf7d8e65b0b706e159d5c818", EXPIRED),
            Map.entry("77c914a6b04c1d40398a602a7c3422eff22ab1850164025dd1f84d3a4f6d3049", EXPIRED),
            Map.entry("15f682d852dc4dfb4830c0d0a8b1c0bcfd0e400c8254c8c63f6c0679fc44769a", EXPIRED),
            Map.entry("7d3f3bfc70270386b236852bd3d3f1a729f7d3bf3d8ac468473bdf15616e995c", EXPIRED),
            Map.entry("01eba40b3a29d75b2a619e6abddcd52775ad22d5009796381e29d8a0727eeeb8", EXPIRED),
            Map.entry("d85a3c22d9afb0c84fd0254cd74991288934e0254e9185db3429e2ea498a33ff", EXPIRED),
            Map.entry("4ea6ac7b1c2371fe9d37c7e96ee5dfa495c4c1ca9477fc2e69fd8426033472cb", EXPIRED),
            Map.entry("a6d7637acb3f3566c5c252cf05ca27d55dbf5c739c6b060692aa2f681b956a2b", EXPIRED),
            Map.entry("ad634c358a223a7a5d412509777816c196e89f9bd853bf5c432c499a27e649f5", EXPIRED),
            Map.entry("0a5869addc9f30ca9f809fefeeee39b9f4d49d8f844efc742a518bdae5acd941", EXPIRED),
            Map.entry("52f9afb8f5c671a41962511d94a9c2c91f38ce072fa7c6caf4733516ece61e59", EXPIRED),
            Map.entry("6f263e588ef2b167d63fde95928da89d39b34bc9a762ac7982ce4fef952503c7", EXPIRED),
            Map.entry("af73a26326a8966d9260e777b407b0c17952137483da10cdca18c70dd8a0db6c", EXPIRED),
            Map.entry("c1c9498f558985e41ad55abab34e74f82038e305a8d47a5d1964e3b8c7a0d199", EXPIRED),
            Map.entry("89c16dd7de18812d08957fad337ff2f39049a0de05f058ac0b7efc7d0f0d2434", EXPIRED),
            Map.entry("f302b3bc52f9c4d3c00118ce36c0604b8393fcf42ac5a7026d4c9090d751bf4c", EXPIRED),
            Map.entry("fff463d0096fbbf1556161af00152e38ac5d9bf4babbf339965da0c965b6b7cf", EXPIRED),
            Map.entry("ab8131b62852160252155cef93a2852196310a90b79aa0bbc266827912d5e01e", EXPIRED),
            Map.entry("f156e281eee7f2055a69079aa682f2a400c00e0c89ab130409dc748c7ad75b18", EXPIRED),
            Map.entry("2f478312e69ee0f596fa7bfaa50ae234821cb3a301be457af00a8129b76c0a72", EXPIRED),
            Map.entry("f7b7113e2fb788a111aee9d81b3f18a1d7b526d26a67a731d75e075d237ad9e2", EXPIRED),
            Map.entry("cb024cec55d373c82df5ca658ea406bb7d23a2da779bcef6823c007e06ce8287", EXPIRED),
            Map.entry("68b03e4c46eae1d03cb0a1d92e137f82b96478956b2a4fa36ca1579edcc44335", EXPIRED),
            Map.entry("de18f1f0b952d61a84b89a481d7d509d723d590c07580afc0a299c75452490c3", EXPIRED),
            Map.entry("7e94f826827f237cf54d494eec82344821d0b220a701df25d9bb730139ba13c9", EXPIRED),
            Map.entry("2d2c357e3bf20714411c53c6955693aa19dce8f16b2f6775fc9731c22ac07889", EXPIRED),
            Map.entry("bb31c66115253a77f1c7ad3ed6c56a19ca03d4b127e6fbc6bf1d1f4daacc57e6", EXPIRED),
            Map.entry("2b0a115ea356f592eeaf27b19fd1962689dffa308aeecccb0c05911b7fbd871e", EXPIRED),
            Map.entry("a122f41a40681beb5d8c6df7a6375fc5c9ac36bc8aaceb4c881fc305c1eb0a20", EXPIRED),
            Map.entry("10241c768e7b3527869ae3a4730f5d991f492d2ef9e57c6c9195b08a38ad9183", EXPIRED),
            Map.entry("53f0beb325d4342c048b3d927906c4250fae888f8f0056e8a62892fdbce12ed7", EXPIRED),
            Map.entry("b4def375a5c9ea6ad7c0b9941be672838b27814a4fc6d76cd4d55a869f540864", EXPIRED),
            Map.entry("9ec05a93263d195898d33d124255b681d564e1ee4018d81c90d29de7158ffcfa", EXPIRED),
            Map.entry("ba6ca449c43e4097677b1be36d89f0cddd2e206dfea1bdfef3877341d6d54a38", EXPIRED),
            Map.entry("c8d114dde77701effae2bf5cac98930f74d3850c17581bdb5e64275e827aea2c", EXPIRED),
            Map.entry("d9ae087ff27f4f8665ddcdda424e7021ea5903e2372732af2e0fd6ca32289c8e", EXPIRED),
            Map.entry("5be85ad833442f8f1378fe77b86def52de1a1f919e200063b7b4b244db6cd822", EXPIRED),
            Map.entry("81752c4dbe734f16a125debba87c1cd7dd9007cbe6a9263876be970f1e563e3a", EXPIRED),
            Map.entry("7279bb687f970c7002a2872a0b4d9c631537cc3c120895d8dba24e4b2c328358", EXPIRED),
            Map.entry("6571f269d6f6a6c820e46d54b1bb603167b38059f5647ec72a679857d0b35273", EXPIRED),
            Map.entry("be1f45c033d21c0df3b09a83c00c65c327e6e1e935bffc8cdc40904aeb2f0634", EXPIRED),
            Map.entry("6aadf91e2b6abb4db74f0c158315d33c8b5da505caac342cc2ff2c20599af1f3", EXPIRED),
            Map.entry("cf155fccfa78d45dcb8ed1864a6a4f103bf1f39263b2d7acac6ee4965afa8be7", EXPIRED),
            Map.entry("16baab7afc0c030f69ab50d9df1945a0c6e6381572e3c99235dc810cb2b82682", EXPIRED),
            Map.entry("a81b11417314664f52cf98f302375c2185717124d45318edeb77a1d3ec40c25f", EXPIRED),
            Map.entry("2f73c8b3eaa602011fec7a081e7c712df8ef12d67f328b8e5e58a8548e22f840", EXPIRED),
            Map.entry("ac4969409175c64eefc885770f1378f56c8491236fb105319d9f48f037a03c8f", EXPIRED),
            Map.entry("602696f0ea936bfe0d331d694fc9eba7760c8747974ee1edbdafb371b0f9680f", EXPIRED),
            Map.entry("98c34dd8b8e9d73862e095aea7ae1f259856a06ff63e78e2bb2dba33e56b7f3e", EXPIRED),
            Map.entry("815e7855f6d368620d993455993992fd6ac033a836155871d817cd6f9b5a59c2", EXPIRED),
            Map.entry("6017293796d20658ae8a39535961e6460ccd65bce7f7dd1a10259c52500287b3", EXPIRED),
            Map.entry("ad300cbe3673033316b78227a3f3a89d5ff3bf9db5f51b1be0de913551788999", EXPIRED),
            Map.entry("a47c23d69a88b14e2fe2177b39165c1c20f45dd6a16fb54f20d448c6b830f9b4", EXPIRED)
        );
    }

    static Optional<ZonedDateTime> overrideDateForLicense(String licenseUidHash) {
        return Optional.ofNullable(LICENSE_OVERRIDES.get(licenseUidHash));
    }
}
