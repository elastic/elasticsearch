/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
/*
 * @(#)FastDoubleMath.java
 * Copyright 2021. Werner Randelshofer, Switzerland. MIT License.
 */

//package ch.randelshofer.fastdoubleparser;
package com.elasticsearch.jdk.boot.math;

import java.util.Objects;

/**
 * This class provides the mathematical functions needed by {@link FastDoubleParser}.
 * <p>
 * This is a C++ to Java port of Daniel Lemire's fast_double_parser.
 * <p>
 * The code contains enhancements from Daniel Lemire's fast_float_parser,
 * so that it can parse double Strings with very long sequences of numbers
 * <p>
 * References:
 * <dl>
 *     <dt>Daniel Lemire, fast_double_parser, 4x faster than strtod.
 *     Apache License 2.0 or Boost Software License.</dt>
 *     <dd><a href="https://github.com/lemire/fast_double_parser">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, fast_float number parsing library: 4x faster than strtod.
 *     Apache License 2.0.</dt>
 *     <dd><a href="https://github.com/fastfloat/fast_float">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, Number Parsing at a Gigabyte per Second,
 *     Software: Practice and Experience 51 (8), 2021.
 *     arXiv.2101.11408v3 [cs.DS] 24 Feb 2021</dt>
 *     <dd><a href="https://arxiv.org/pdf/2101.11408.pdf">arxiv.org</a></dd>
 * </dl>
 */
class FastDoubleMath {
    /**
     * The smallest non-zero float (binary64) is 2 ^ -1074.
     * We take as input numbers of the form w x 10^q where w &lt; 2^64.
     * We have that {@literal w * 10^-343 < 2^(64-344) 5^-343 &lt; 2^-1076}.
     * <p>
     * However, we have that
     * {@literal (2^64-1) * 10^-342 = (2^64-1) * 2^-342 * 5^-342 > 2 ^ -1074}.
     * Thus it is possible for a number of the form w * 10^-342 where
     * w is a 64-bit value to be a non-zero floating-point number.
     * <p>
     * ********
     * <p>
     * If we are solely interested in the *normal* numbers then the
     * smallest value is 2^-1022. We can generate a value larger
     * than 2^-1022 with expressions of the form w * 10^-326.
     * Thus we need to pick FASTFLOAT_SMALLEST_POWER >= -326.
     * <p>
     * ********
     * <p>
     * Any number of form w * 10^309 where w>= 1 is going to be
     * infinite in binary64 so we never need to worry about powers
     * of 5 greater than 308.
     */
    private final static int FASTFLOAT_DEC_SMALLEST_POWER = -325;
    private final static int FASTFLOAT_DEC_LARGEST_POWER = 308;
    private final static int FASTFLOAT_HEX_SMALLEST_POWER = Double.MIN_EXPONENT;
    private final static int FASTFLOAT_HEX_LARGEST_POWER = Double.MAX_EXPONENT;
    /**
     * Precomputed powers of ten from 10^0 to 10^22. These
     * can be represented exactly using the double type.
     */
    private static final double[] powerOfTen = {
        1e0,
        1e1,
        1e2,
        1e3,
        1e4,
        1e5,
        1e6,
        1e7,
        1e8,
        1e9,
        1e10,
        1e11,
        1e12,
        1e13,
        1e14,
        1e15,
        1e16,
        1e17,
        1e18,
        1e19,
        1e20,
        1e21,
        1e22 };
    /**
     * When mapping numbers from decimal to binary,
     * we go from w * 10^q to m * 2^p but we have
     * 10^q = 5^q * 2^q, so effectively
     * we are trying to match
     * w * 2^q * 5^q to m * 2^p. Thus the powers of two
     * are not a concern since they can be represented
     * exactly using the binary notation, only the powers of five
     * affect the binary significand.
     * <p>
     * The mantissas of powers of ten from -308 to 308, extended out to sixty four
     * bits. The array contains the powers of ten approximated
     * as a 64-bit mantissa. It goes from 10^FASTFLOAT_SMALLEST_POWER to
     * 10^FASTFLOAT_LARGEST_POWER (inclusively).
     * The mantissa is truncated, and
     * never rounded up. Uses about 5KB.
     */
    private static final long[] MANTISSA_64 = {
        0xa5ced43b7e3e9188L,
        0xcf42894a5dce35eaL,
        0x818995ce7aa0e1b2L,
        0xa1ebfb4219491a1fL,
        0xca66fa129f9b60a6L,
        0xfd00b897478238d0L,
        0x9e20735e8cb16382L,
        0xc5a890362fddbc62L,
        0xf712b443bbd52b7bL,
        0x9a6bb0aa55653b2dL,
        0xc1069cd4eabe89f8L,
        0xf148440a256e2c76L,
        0x96cd2a865764dbcaL,
        0xbc807527ed3e12bcL,
        0xeba09271e88d976bL,
        0x93445b8731587ea3L,
        0xb8157268fdae9e4cL,
        0xe61acf033d1a45dfL,
        0x8fd0c16206306babL,
        0xb3c4f1ba87bc8696L,
        0xe0b62e2929aba83cL,
        0x8c71dcd9ba0b4925L,
        0xaf8e5410288e1b6fL,
        0xdb71e91432b1a24aL,
        0x892731ac9faf056eL,
        0xab70fe17c79ac6caL,
        0xd64d3d9db981787dL,
        0x85f0468293f0eb4eL,
        0xa76c582338ed2621L,
        0xd1476e2c07286faaL,
        0x82cca4db847945caL,
        0xa37fce126597973cL,
        0xcc5fc196fefd7d0cL,
        0xff77b1fcbebcdc4fL,
        0x9faacf3df73609b1L,
        0xc795830d75038c1dL,
        0xf97ae3d0d2446f25L,
        0x9becce62836ac577L,
        0xc2e801fb244576d5L,
        0xf3a20279ed56d48aL,
        0x9845418c345644d6L,
        0xbe5691ef416bd60cL,
        0xedec366b11c6cb8fL,
        0x94b3a202eb1c3f39L,
        0xb9e08a83a5e34f07L,
        0xe858ad248f5c22c9L,
        0x91376c36d99995beL,
        0xb58547448ffffb2dL,
        0xe2e69915b3fff9f9L,
        0x8dd01fad907ffc3bL,
        0xb1442798f49ffb4aL,
        0xdd95317f31c7fa1dL,
        0x8a7d3eef7f1cfc52L,
        0xad1c8eab5ee43b66L,
        0xd863b256369d4a40L,
        0x873e4f75e2224e68L,
        0xa90de3535aaae202L,
        0xd3515c2831559a83L,
        0x8412d9991ed58091L,
        0xa5178fff668ae0b6L,
        0xce5d73ff402d98e3L,
        0x80fa687f881c7f8eL,
        0xa139029f6a239f72L,
        0xc987434744ac874eL,
        0xfbe9141915d7a922L,
        0x9d71ac8fada6c9b5L,
        0xc4ce17b399107c22L,
        0xf6019da07f549b2bL,
        0x99c102844f94e0fbL,
        0xc0314325637a1939L,
        0xf03d93eebc589f88L,
        0x96267c7535b763b5L,
        0xbbb01b9283253ca2L,
        0xea9c227723ee8bcbL,
        0x92a1958a7675175fL,
        0xb749faed14125d36L,
        0xe51c79a85916f484L,
        0x8f31cc0937ae58d2L,
        0xb2fe3f0b8599ef07L,
        0xdfbdcece67006ac9L,
        0x8bd6a141006042bdL,
        0xaecc49914078536dL,
        0xda7f5bf590966848L,
        0x888f99797a5e012dL,
        0xaab37fd7d8f58178L,
        0xd5605fcdcf32e1d6L,
        0x855c3be0a17fcd26L,
        0xa6b34ad8c9dfc06fL,
        0xd0601d8efc57b08bL,
        0x823c12795db6ce57L,
        0xa2cb1717b52481edL,
        0xcb7ddcdda26da268L,
        0xfe5d54150b090b02L,
        0x9efa548d26e5a6e1L,
        0xc6b8e9b0709f109aL,
        0xf867241c8cc6d4c0L,
        0x9b407691d7fc44f8L,
        0xc21094364dfb5636L,
        0xf294b943e17a2bc4L,
        0x979cf3ca6cec5b5aL,
        0xbd8430bd08277231L,
        0xece53cec4a314ebdL,
        0x940f4613ae5ed136L,
        0xb913179899f68584L,
        0xe757dd7ec07426e5L,
        0x9096ea6f3848984fL,
        0xb4bca50b065abe63L,
        0xe1ebce4dc7f16dfbL,
        0x8d3360f09cf6e4bdL,
        0xb080392cc4349decL,
        0xdca04777f541c567L,
        0x89e42caaf9491b60L,
        0xac5d37d5b79b6239L,
        0xd77485cb25823ac7L,
        0x86a8d39ef77164bcL,
        0xa8530886b54dbdebL,
        0xd267caa862a12d66L,
        0x8380dea93da4bc60L,
        0xa46116538d0deb78L,
        0xcd795be870516656L,
        0x806bd9714632dff6L,
        0xa086cfcd97bf97f3L,
        0xc8a883c0fdaf7df0L,
        0xfad2a4b13d1b5d6cL,
        0x9cc3a6eec6311a63L,
        0xc3f490aa77bd60fcL,
        0xf4f1b4d515acb93bL,
        0x991711052d8bf3c5L,
        0xbf5cd54678eef0b6L,
        0xef340a98172aace4L,
        0x9580869f0e7aac0eL,
        0xbae0a846d2195712L,
        0xe998d258869facd7L,
        0x91ff83775423cc06L,
        0xb67f6455292cbf08L,
        0xe41f3d6a7377eecaL,
        0x8e938662882af53eL,
        0xb23867fb2a35b28dL,
        0xdec681f9f4c31f31L,
        0x8b3c113c38f9f37eL,
        0xae0b158b4738705eL,
        0xd98ddaee19068c76L,
        0x87f8a8d4cfa417c9L,
        0xa9f6d30a038d1dbcL,
        0xd47487cc8470652bL,
        0x84c8d4dfd2c63f3bL,
        0xa5fb0a17c777cf09L,
        0xcf79cc9db955c2ccL,
        0x81ac1fe293d599bfL,
        0xa21727db38cb002fL,
        0xca9cf1d206fdc03bL,
        0xfd442e4688bd304aL,
        0x9e4a9cec15763e2eL,
        0xc5dd44271ad3cdbaL,
        0xf7549530e188c128L,
        0x9a94dd3e8cf578b9L,
        0xc13a148e3032d6e7L,
        0xf18899b1bc3f8ca1L,
        0x96f5600f15a7b7e5L,
        0xbcb2b812db11a5deL,
        0xebdf661791d60f56L,
        0x936b9fcebb25c995L,
        0xb84687c269ef3bfbL,
        0xe65829b3046b0afaL,
        0x8ff71a0fe2c2e6dcL,
        0xb3f4e093db73a093L,
        0xe0f218b8d25088b8L,
        0x8c974f7383725573L,
        0xafbd2350644eeacfL,
        0xdbac6c247d62a583L,
        0x894bc396ce5da772L,
        0xab9eb47c81f5114fL,
        0xd686619ba27255a2L,
        0x8613fd0145877585L,
        0xa798fc4196e952e7L,
        0xd17f3b51fca3a7a0L,
        0x82ef85133de648c4L,
        0xa3ab66580d5fdaf5L,
        0xcc963fee10b7d1b3L,
        0xffbbcfe994e5c61fL,
        0x9fd561f1fd0f9bd3L,
        0xc7caba6e7c5382c8L,
        0xf9bd690a1b68637bL,
        0x9c1661a651213e2dL,
        0xc31bfa0fe5698db8L,
        0xf3e2f893dec3f126L,
        0x986ddb5c6b3a76b7L,
        0xbe89523386091465L,
        0xee2ba6c0678b597fL,
        0x94db483840b717efL,
        0xba121a4650e4ddebL,
        0xe896a0d7e51e1566L,
        0x915e2486ef32cd60L,
        0xb5b5ada8aaff80b8L,
        0xe3231912d5bf60e6L,
        0x8df5efabc5979c8fL,
        0xb1736b96b6fd83b3L,
        0xddd0467c64bce4a0L,
        0x8aa22c0dbef60ee4L,
        0xad4ab7112eb3929dL,
        0xd89d64d57a607744L,
        0x87625f056c7c4a8bL,
        0xa93af6c6c79b5d2dL,
        0xd389b47879823479L,
        0x843610cb4bf160cbL,
        0xa54394fe1eedb8feL,
        0xce947a3da6a9273eL,
        0x811ccc668829b887L,
        0xa163ff802a3426a8L,
        0xc9bcff6034c13052L,
        0xfc2c3f3841f17c67L,
        0x9d9ba7832936edc0L,
        0xc5029163f384a931L,
        0xf64335bcf065d37dL,
        0x99ea0196163fa42eL,
        0xc06481fb9bcf8d39L,
        0xf07da27a82c37088L,
        0x964e858c91ba2655L,
        0xbbe226efb628afeaL,
        0xeadab0aba3b2dbe5L,
        0x92c8ae6b464fc96fL,
        0xb77ada0617e3bbcbL,
        0xe55990879ddcaabdL,
        0x8f57fa54c2a9eab6L,
        0xb32df8e9f3546564L,
        0xdff9772470297ebdL,
        0x8bfbea76c619ef36L,
        0xaefae51477a06b03L,
        0xdab99e59958885c4L,
        0x88b402f7fd75539bL,
        0xaae103b5fcd2a881L,
        0xd59944a37c0752a2L,
        0x857fcae62d8493a5L,
        0xa6dfbd9fb8e5b88eL,
        0xd097ad07a71f26b2L,
        0x825ecc24c873782fL,
        0xa2f67f2dfa90563bL,
        0xcbb41ef979346bcaL,
        0xfea126b7d78186bcL,
        0x9f24b832e6b0f436L,
        0xc6ede63fa05d3143L,
        0xf8a95fcf88747d94L,
        0x9b69dbe1b548ce7cL,
        0xc24452da229b021bL,
        0xf2d56790ab41c2a2L,
        0x97c560ba6b0919a5L,
        0xbdb6b8e905cb600fL,
        0xed246723473e3813L,
        0x9436c0760c86e30bL,
        0xb94470938fa89bceL,
        0xe7958cb87392c2c2L,
        0x90bd77f3483bb9b9L,
        0xb4ecd5f01a4aa828L,
        0xe2280b6c20dd5232L,
        0x8d590723948a535fL,
        0xb0af48ec79ace837L,
        0xdcdb1b2798182244L,
        0x8a08f0f8bf0f156bL,
        0xac8b2d36eed2dac5L,
        0xd7adf884aa879177L,
        0x86ccbb52ea94baeaL,
        0xa87fea27a539e9a5L,
        0xd29fe4b18e88640eL,
        0x83a3eeeef9153e89L,
        0xa48ceaaab75a8e2bL,
        0xcdb02555653131b6L,
        0x808e17555f3ebf11L,
        0xa0b19d2ab70e6ed6L,
        0xc8de047564d20a8bL,
        0xfb158592be068d2eL,
        0x9ced737bb6c4183dL,
        0xc428d05aa4751e4cL,
        0xf53304714d9265dfL,
        0x993fe2c6d07b7fabL,
        0xbf8fdb78849a5f96L,
        0xef73d256a5c0f77cL,
        0x95a8637627989aadL,
        0xbb127c53b17ec159L,
        0xe9d71b689dde71afL,
        0x9226712162ab070dL,
        0xb6b00d69bb55c8d1L,
        0xe45c10c42a2b3b05L,
        0x8eb98a7a9a5b04e3L,
        0xb267ed1940f1c61cL,
        0xdf01e85f912e37a3L,
        0x8b61313bbabce2c6L,
        0xae397d8aa96c1b77L,
        0xd9c7dced53c72255L,
        0x881cea14545c7575L,
        0xaa242499697392d2L,
        0xd4ad2dbfc3d07787L,
        0x84ec3c97da624ab4L,
        0xa6274bbdd0fadd61L,
        0xcfb11ead453994baL,
        0x81ceb32c4b43fcf4L,
        0xa2425ff75e14fc31L,
        0xcad2f7f5359a3b3eL,
        0xfd87b5f28300ca0dL,
        0x9e74d1b791e07e48L,
        0xc612062576589ddaL,
        0xf79687aed3eec551L,
        0x9abe14cd44753b52L,
        0xc16d9a0095928a27L,
        0xf1c90080baf72cb1L,
        0x971da05074da7beeL,
        0xbce5086492111aeaL,
        0xec1e4a7db69561a5L,
        0x9392ee8e921d5d07L,
        0xb877aa3236a4b449L,
        0xe69594bec44de15bL,
        0x901d7cf73ab0acd9L,
        0xb424dc35095cd80fL,
        0xe12e13424bb40e13L,
        0x8cbccc096f5088cbL,
        0xafebff0bcb24aafeL,
        0xdbe6fecebdedd5beL,
        0x89705f4136b4a597L,
        0xabcc77118461cefcL,
        0xd6bf94d5e57a42bcL,
        0x8637bd05af6c69b5L,
        0xa7c5ac471b478423L,
        0xd1b71758e219652bL,
        0x83126e978d4fdf3bL,
        0xa3d70a3d70a3d70aL,
        0xccccccccccccccccL,
        0x8000000000000000L,
        0xa000000000000000L,
        0xc800000000000000L,
        0xfa00000000000000L,
        0x9c40000000000000L,
        0xc350000000000000L,
        0xf424000000000000L,
        0x9896800000000000L,
        0xbebc200000000000L,
        0xee6b280000000000L,
        0x9502f90000000000L,
        0xba43b74000000000L,
        0xe8d4a51000000000L,
        0x9184e72a00000000L,
        0xb5e620f480000000L,
        0xe35fa931a0000000L,
        0x8e1bc9bf04000000L,
        0xb1a2bc2ec5000000L,
        0xde0b6b3a76400000L,
        0x8ac7230489e80000L,
        0xad78ebc5ac620000L,
        0xd8d726b7177a8000L,
        0x878678326eac9000L,
        0xa968163f0a57b400L,
        0xd3c21bcecceda100L,
        0x84595161401484a0L,
        0xa56fa5b99019a5c8L,
        0xcecb8f27f4200f3aL,
        0x813f3978f8940984L,
        0xa18f07d736b90be5L,
        0xc9f2c9cd04674edeL,
        0xfc6f7c4045812296L,
        0x9dc5ada82b70b59dL,
        0xc5371912364ce305L,
        0xf684df56c3e01bc6L,
        0x9a130b963a6c115cL,
        0xc097ce7bc90715b3L,
        0xf0bdc21abb48db20L,
        0x96769950b50d88f4L,
        0xbc143fa4e250eb31L,
        0xeb194f8e1ae525fdL,
        0x92efd1b8d0cf37beL,
        0xb7abc627050305adL,
        0xe596b7b0c643c719L,
        0x8f7e32ce7bea5c6fL,
        0xb35dbf821ae4f38bL,
        0xe0352f62a19e306eL,
        0x8c213d9da502de45L,
        0xaf298d050e4395d6L,
        0xdaf3f04651d47b4cL,
        0x88d8762bf324cd0fL,
        0xab0e93b6efee0053L,
        0xd5d238a4abe98068L,
        0x85a36366eb71f041L,
        0xa70c3c40a64e6c51L,
        0xd0cf4b50cfe20765L,
        0x82818f1281ed449fL,
        0xa321f2d7226895c7L,
        0xcbea6f8ceb02bb39L,
        0xfee50b7025c36a08L,
        0x9f4f2726179a2245L,
        0xc722f0ef9d80aad6L,
        0xf8ebad2b84e0d58bL,
        0x9b934c3b330c8577L,
        0xc2781f49ffcfa6d5L,
        0xf316271c7fc3908aL,
        0x97edd871cfda3a56L,
        0xbde94e8e43d0c8ecL,
        0xed63a231d4c4fb27L,
        0x945e455f24fb1cf8L,
        0xb975d6b6ee39e436L,
        0xe7d34c64a9c85d44L,
        0x90e40fbeea1d3a4aL,
        0xb51d13aea4a488ddL,
        0xe264589a4dcdab14L,
        0x8d7eb76070a08aecL,
        0xb0de65388cc8ada8L,
        0xdd15fe86affad912L,
        0x8a2dbf142dfcc7abL,
        0xacb92ed9397bf996L,
        0xd7e77a8f87daf7fbL,
        0x86f0ac99b4e8dafdL,
        0xa8acd7c0222311bcL,
        0xd2d80db02aabd62bL,
        0x83c7088e1aab65dbL,
        0xa4b8cab1a1563f52L,
        0xcde6fd5e09abcf26L,
        0x80b05e5ac60b6178L,
        0xa0dc75f1778e39d6L,
        0xc913936dd571c84cL,
        0xfb5878494ace3a5fL,
        0x9d174b2dcec0e47bL,
        0xc45d1df942711d9aL,
        0xf5746577930d6500L,
        0x9968bf6abbe85f20L,
        0xbfc2ef456ae276e8L,
        0xefb3ab16c59b14a2L,
        0x95d04aee3b80ece5L,
        0xbb445da9ca61281fL,
        0xea1575143cf97226L,
        0x924d692ca61be758L,
        0xb6e0c377cfa2e12eL,
        0xe498f455c38b997aL,
        0x8edf98b59a373fecL,
        0xb2977ee300c50fe7L,
        0xdf3d5e9bc0f653e1L,
        0x8b865b215899f46cL,
        0xae67f1e9aec07187L,
        0xda01ee641a708de9L,
        0x884134fe908658b2L,
        0xaa51823e34a7eedeL,
        0xd4e5e2cdc1d1ea96L,
        0x850fadc09923329eL,
        0xa6539930bf6bff45L,
        0xcfe87f7cef46ff16L,
        0x81f14fae158c5f6eL,
        0xa26da3999aef7749L,
        0xcb090c8001ab551cL,
        0xfdcb4fa002162a63L,
        0x9e9f11c4014dda7eL,
        0xc646d63501a1511dL,
        0xf7d88bc24209a565L,
        0x9ae757596946075fL,
        0xc1a12d2fc3978937L,
        0xf209787bb47d6b84L,
        0x9745eb4d50ce6332L,
        0xbd176620a501fbffL,
        0xec5d3fa8ce427affL,
        0x93ba47c980e98cdfL,
        0xb8a8d9bbe123f017L,
        0xe6d3102ad96cec1dL,
        0x9043ea1ac7e41392L,
        0xb454e4a179dd1877L,
        0xe16a1dc9d8545e94L,
        0x8ce2529e2734bb1dL,
        0xb01ae745b101e9e4L,
        0xdc21a1171d42645dL,
        0x899504ae72497ebaL,
        0xabfa45da0edbde69L,
        0xd6f8d7509292d603L,
        0x865b86925b9bc5c2L,
        0xa7f26836f282b732L,
        0xd1ef0244af2364ffL,
        0x8335616aed761f1fL,
        0xa402b9c5a8d3a6e7L,
        0xcd036837130890a1L,
        0x802221226be55a64L,
        0xa02aa96b06deb0fdL,
        0xc83553c5c8965d3dL,
        0xfa42a8b73abbf48cL,
        0x9c69a97284b578d7L,
        0xc38413cf25e2d70dL,
        0xf46518c2ef5b8cd1L,
        0x98bf2f79d5993802L,
        0xbeeefb584aff8603L,
        0xeeaaba2e5dbf6784L,
        0x952ab45cfa97a0b2L,
        0xba756174393d88dfL,
        0xe912b9d1478ceb17L,
        0x91abb422ccb812eeL,
        0xb616a12b7fe617aaL,
        0xe39c49765fdf9d94L,
        0x8e41ade9fbebc27dL,
        0xb1d219647ae6b31cL,
        0xde469fbd99a05fe3L,
        0x8aec23d680043beeL,
        0xada72ccc20054ae9L,
        0xd910f7ff28069da4L,
        0x87aa9aff79042286L,
        0xa99541bf57452b28L,
        0xd3fa922f2d1675f2L,
        0x847c9b5d7c2e09b7L,
        0xa59bc234db398c25L,
        0xcf02b2c21207ef2eL,
        0x8161afb94b44f57dL,
        0xa1ba1ba79e1632dcL,
        0xca28a291859bbf93L,
        0xfcb2cb35e702af78L,
        0x9defbf01b061adabL,
        0xc56baec21c7a1916L,
        0xf6c69a72a3989f5bL,
        0x9a3c2087a63f6399L,
        0xc0cb28a98fcf3c7fL,
        0xf0fdf2d3f3c30b9fL,
        0x969eb7c47859e743L,
        0xbc4665b596706114L,
        0xeb57ff22fc0c7959L,
        0x9316ff75dd87cbd8L,
        0xb7dcbf5354e9beceL,
        0xe5d3ef282a242e81L,
        0x8fa475791a569d10L,
        0xb38d92d760ec4455L,
        0xe070f78d3927556aL,
        0x8c469ab843b89562L,
        0xaf58416654a6babbL,
        0xdb2e51bfe9d0696aL,
        0x88fcf317f22241e2L,
        0xab3c2fddeeaad25aL,
        0xd60b3bd56a5586f1L,
        0x85c7056562757456L,
        0xa738c6bebb12d16cL,
        0xd106f86e69d785c7L,
        0x82a45b450226b39cL,
        0xa34d721642b06084L,
        0xcc20ce9bd35c78a5L,
        0xff290242c83396ceL,
        0x9f79a169bd203e41L,
        0xc75809c42c684dd1L,
        0xf92e0c3537826145L,
        0x9bbcc7a142b17ccbL,
        0xc2abf989935ddbfeL,
        0xf356f7ebf83552feL,
        0x98165af37b2153deL,
        0xbe1bf1b059e9a8d6L,
        0xeda2ee1c7064130cL,
        0x9485d4d1c63e8be7L,
        0xb9a74a0637ce2ee1L,
        0xe8111c87c5c1ba99L,
        0x910ab1d4db9914a0L,
        0xb54d5e4a127f59c8L,
        0xe2a0b5dc971f303aL,
        0x8da471a9de737e24L,
        0xb10d8e1456105dadL,
        0xdd50f1996b947518L,
        0x8a5296ffe33cc92fL,
        0xace73cbfdc0bfb7bL,
        0xd8210befd30efa5aL,
        0x8714a775e3e95c78L,
        0xa8d9d1535ce3b396L,
        0xd31045a8341ca07cL,
        0x83ea2b892091e44dL,
        0xa4e4b66b68b65d60L,
        0xce1de40642e3f4b9L,
        0x80d2ae83e9ce78f3L,
        0xa1075a24e4421730L,
        0xc94930ae1d529cfcL,
        0xfb9b7cd9a4a7443cL,
        0x9d412e0806e88aa5L,
        0xc491798a08a2ad4eL,
        0xf5b5d7ec8acb58a2L,
        0x9991a6f3d6bf1765L,
        0xbff610b0cc6edd3fL,
        0xeff394dcff8a948eL,
        0x95f83d0a1fb69cd9L,
        0xbb764c4ca7a4440fL,
        0xea53df5fd18d5513L,
        0x92746b9be2f8552cL,
        0xb7118682dbb66a77L,
        0xe4d5e82392a40515L,
        0x8f05b1163ba6832dL,
        0xb2c71d5bca9023f8L,
        0xdf78e4b2bd342cf6L,
        0x8bab8eefb6409c1aL,
        0xae9672aba3d0c320L,
        0xda3c0f568cc4f3e8L,
        0x8865899617fb1871L,
        0xaa7eebfb9df9de8dL,
        0xd51ea6fa85785631L,
        0x8533285c936b35deL,
        0xa67ff273b8460356L,
        0xd01fef10a657842cL,
        0x8213f56a67f6b29bL,
        0xa298f2c501f45f42L,
        0xcb3f2f7642717713L,
        0xfe0efb53d30dd4d7L,
        0x9ec95d1463e8a506L,
        0xc67bb4597ce2ce48L,
        0xf81aa16fdc1b81daL,
        0x9b10a4e5e9913128L,
        0xc1d4ce1f63f57d72L,
        0xf24a01a73cf2dccfL,
        0x976e41088617ca01L,
        0xbd49d14aa79dbc82L,
        0xec9c459d51852ba2L,
        0x93e1ab8252f33b45L,
        0xb8da1662e7b00a17L,
        0xe7109bfba19c0c9dL,
        0x906a617d450187e2L,
        0xb484f9dc9641e9daL,
        0xe1a63853bbd26451L,
        0x8d07e33455637eb2L,
        0xb049dc016abc5e5fL,
        0xdc5c5301c56b75f7L,
        0x89b9b3e11b6329baL,
        0xac2820d9623bf429L,
        0xd732290fbacaf133L,
        0x867f59a9d4bed6c0L,
        0xa81f301449ee8c70L,
        0xd226fc195c6a2f8cL,
        0x83585d8fd9c25db7L,
        0xa42e74f3d032f525L,
        0xcd3a1230c43fb26fL,
        0x80444b5e7aa7cf85L,
        0xa0555e361951c366L,
        0xc86ab5c39fa63440L,
        0xfa856334878fc150L,
        0x9c935e00d4b9d8d2L,
        0xc3b8358109e84f07L,
        0xf4a642e14c6262c8L,
        0x98e7e9cccfbd7dbdL,
        0xbf21e44003acdd2cL,
        0xeeea5d5004981478L,
        0x95527a5202df0ccbL,
        0xbaa718e68396cffdL,
        0xe950df20247c83fdL,
        0x91d28b7416cdd27eL,
        0xb6472e511c81471dL,
        0xe3d8f9e563a198e5L,
        0x8e679c2f5e44ff8fL };
    /**
     * A complement to mantissa_64
     * complete to a 128-bit mantissa.
     * Uses about 5KB but is rarely accessed.
     */
    private final static long[] MANTISSA_128 = {
        0x419ea3bd35385e2dL,
        0x52064cac828675b9L,
        0x7343efebd1940993L,
        0x1014ebe6c5f90bf8L,
        0xd41a26e077774ef6L,
        0x8920b098955522b4L,
        0x55b46e5f5d5535b0L,
        0xeb2189f734aa831dL,
        0xa5e9ec7501d523e4L,
        0x47b233c92125366eL,
        0x999ec0bb696e840aL,
        0xc00670ea43ca250dL,
        0x380406926a5e5728L,
        0xc605083704f5ecf2L,
        0xf7864a44c633682eL,
        0x7ab3ee6afbe0211dL,
        0x5960ea05bad82964L,
        0x6fb92487298e33bdL,
        0xa5d3b6d479f8e056L,
        0x8f48a4899877186cL,
        0x331acdabfe94de87L,
        0x9ff0c08b7f1d0b14L,
        0x7ecf0ae5ee44dd9L,
        0xc9e82cd9f69d6150L,
        0xbe311c083a225cd2L,
        0x6dbd630a48aaf406L,
        0x92cbbccdad5b108L,
        0x25bbf56008c58ea5L,
        0xaf2af2b80af6f24eL,
        0x1af5af660db4aee1L,
        0x50d98d9fc890ed4dL,
        0xe50ff107bab528a0L,
        0x1e53ed49a96272c8L,
        0x25e8e89c13bb0f7aL,
        0x77b191618c54e9acL,
        0xd59df5b9ef6a2417L,
        0x4b0573286b44ad1dL,
        0x4ee367f9430aec32L,
        0x229c41f793cda73fL,
        0x6b43527578c1110fL,
        0x830a13896b78aaa9L,
        0x23cc986bc656d553L,
        0x2cbfbe86b7ec8aa8L,
        0x7bf7d71432f3d6a9L,
        0xdaf5ccd93fb0cc53L,
        0xd1b3400f8f9cff68L,
        0x23100809b9c21fa1L,
        0xabd40a0c2832a78aL,
        0x16c90c8f323f516cL,
        0xae3da7d97f6792e3L,
        0x99cd11cfdf41779cL,
        0x40405643d711d583L,
        0x482835ea666b2572L,
        0xda3243650005eecfL,
        0x90bed43e40076a82L,
        0x5a7744a6e804a291L,
        0x711515d0a205cb36L,
        0xd5a5b44ca873e03L,
        0xe858790afe9486c2L,
        0x626e974dbe39a872L,
        0xfb0a3d212dc8128fL,
        0x7ce66634bc9d0b99L,
        0x1c1fffc1ebc44e80L,
        0xa327ffb266b56220L,
        0x4bf1ff9f0062baa8L,
        0x6f773fc3603db4a9L,
        0xcb550fb4384d21d3L,
        0x7e2a53a146606a48L,
        0x2eda7444cbfc426dL,
        0xfa911155fefb5308L,
        0x793555ab7eba27caL,
        0x4bc1558b2f3458deL,
        0x9eb1aaedfb016f16L,
        0x465e15a979c1cadcL,
        0xbfacd89ec191ec9L,
        0xcef980ec671f667bL,
        0x82b7e12780e7401aL,
        0xd1b2ecb8b0908810L,
        0x861fa7e6dcb4aa15L,
        0x67a791e093e1d49aL,
        0xe0c8bb2c5c6d24e0L,
        0x58fae9f773886e18L,
        0xaf39a475506a899eL,
        0x6d8406c952429603L,
        0xc8e5087ba6d33b83L,
        0xfb1e4a9a90880a64L,
        0x5cf2eea09a55067fL,
        0xf42faa48c0ea481eL,
        0xf13b94daf124da26L,
        0x76c53d08d6b70858L,
        0x54768c4b0c64ca6eL,
        0xa9942f5dcf7dfd09L,
        0xd3f93b35435d7c4cL,
        0xc47bc5014a1a6dafL,
        0x359ab6419ca1091bL,
        0xc30163d203c94b62L,
        0x79e0de63425dcf1dL,
        0x985915fc12f542e4L,
        0x3e6f5b7b17b2939dL,
        0xa705992ceecf9c42L,
        0x50c6ff782a838353L,
        0xa4f8bf5635246428L,
        0x871b7795e136be99L,
        0x28e2557b59846e3fL,
        0x331aeada2fe589cfL,
        0x3ff0d2c85def7621L,
        0xfed077a756b53a9L,
        0xd3e8495912c62894L,
        0x64712dd7abbbd95cL,
        0xbd8d794d96aacfb3L,
        0xecf0d7a0fc5583a0L,
        0xf41686c49db57244L,
        0x311c2875c522ced5L,
        0x7d633293366b828bL,
        0xae5dff9c02033197L,
        0xd9f57f830283fdfcL,
        0xd072df63c324fd7bL,
        0x4247cb9e59f71e6dL,
        0x52d9be85f074e608L,
        0x67902e276c921f8bL,
        0xba1cd8a3db53b6L,
        0x80e8a40eccd228a4L,
        0x6122cd128006b2cdL,
        0x796b805720085f81L,
        0xcbe3303674053bb0L,
        0xbedbfc4411068a9cL,
        0xee92fb5515482d44L,
        0x751bdd152d4d1c4aL,
        0xd262d45a78a0635dL,
        0x86fb897116c87c34L,
        0xd45d35e6ae3d4da0L,
        0x8974836059cca109L,
        0x2bd1a438703fc94bL,
        0x7b6306a34627ddcfL,
        0x1a3bc84c17b1d542L,
        0x20caba5f1d9e4a93L,
        0x547eb47b7282ee9cL,
        0xe99e619a4f23aa43L,
        0x6405fa00e2ec94d4L,
        0xde83bc408dd3dd04L,
        0x9624ab50b148d445L,
        0x3badd624dd9b0957L,
        0xe54ca5d70a80e5d6L,
        0x5e9fcf4ccd211f4cL,
        0x7647c3200069671fL,
        0x29ecd9f40041e073L,
        0xf468107100525890L,
        0x7182148d4066eeb4L,
        0xc6f14cd848405530L,
        0xb8ada00e5a506a7cL,
        0xa6d90811f0e4851cL,
        0x908f4a166d1da663L,
        0x9a598e4e043287feL,
        0x40eff1e1853f29fdL,
        0xd12bee59e68ef47cL,
        0x82bb74f8301958ceL,
        0xe36a52363c1faf01L,
        0xdc44e6c3cb279ac1L,
        0x29ab103a5ef8c0b9L,
        0x7415d448f6b6f0e7L,
        0x111b495b3464ad21L,
        0xcab10dd900beec34L,
        0x3d5d514f40eea742L,
        0xcb4a5a3112a5112L,
        0x47f0e785eaba72abL,
        0x59ed216765690f56L,
        0x306869c13ec3532cL,
        0x1e414218c73a13fbL,
        0xe5d1929ef90898faL,
        0xdf45f746b74abf39L,
        0x6b8bba8c328eb783L,
        0x66ea92f3f326564L,
        0xc80a537b0efefebdL,
        0xbd06742ce95f5f36L,
        0x2c48113823b73704L,
        0xf75a15862ca504c5L,
        0x9a984d73dbe722fbL,
        0xc13e60d0d2e0ebbaL,
        0x318df905079926a8L,
        0xfdf17746497f7052L,
        0xfeb6ea8bedefa633L,
        0xfe64a52ee96b8fc0L,
        0x3dfdce7aa3c673b0L,
        0x6bea10ca65c084eL,
        0x486e494fcff30a62L,
        0x5a89dba3c3efccfaL,
        0xf89629465a75e01cL,
        0xf6bbb397f1135823L,
        0x746aa07ded582e2cL,
        0xa8c2a44eb4571cdcL,
        0x92f34d62616ce413L,
        0x77b020baf9c81d17L,
        0xace1474dc1d122eL,
        0xd819992132456baL,
        0x10e1fff697ed6c69L,
        0xca8d3ffa1ef463c1L,
        0xbd308ff8a6b17cb2L,
        0xac7cb3f6d05ddbdeL,
        0x6bcdf07a423aa96bL,
        0x86c16c98d2c953c6L,
        0xe871c7bf077ba8b7L,
        0x11471cd764ad4972L,
        0xd598e40d3dd89bcfL,
        0x4aff1d108d4ec2c3L,
        0xcedf722a585139baL,
        0xc2974eb4ee658828L,
        0x733d226229feea32L,
        0x806357d5a3f525fL,
        0xca07c2dcb0cf26f7L,
        0xfc89b393dd02f0b5L,
        0xbbac2078d443ace2L,
        0xd54b944b84aa4c0dL,
        0xa9e795e65d4df11L,
        0x4d4617b5ff4a16d5L,
        0x504bced1bf8e4e45L,
        0xe45ec2862f71e1d6L,
        0x5d767327bb4e5a4cL,
        0x3a6a07f8d510f86fL,
        0x890489f70a55368bL,
        0x2b45ac74ccea842eL,
        0x3b0b8bc90012929dL,
        0x9ce6ebb40173744L,
        0xcc420a6a101d0515L,
        0x9fa946824a12232dL,
        0x47939822dc96abf9L,
        0x59787e2b93bc56f7L,
        0x57eb4edb3c55b65aL,
        0xede622920b6b23f1L,
        0xe95fab368e45ecedL,
        0x11dbcb0218ebb414L,
        0xd652bdc29f26a119L,
        0x4be76d3346f0495fL,
        0x6f70a4400c562ddbL,
        0xcb4ccd500f6bb952L,
        0x7e2000a41346a7a7L,
        0x8ed400668c0c28c8L,
        0x728900802f0f32faL,
        0x4f2b40a03ad2ffb9L,
        0xe2f610c84987bfa8L,
        0xdd9ca7d2df4d7c9L,
        0x91503d1c79720dbbL,
        0x75a44c6397ce912aL,
        0xc986afbe3ee11abaL,
        0xfbe85badce996168L,
        0xfae27299423fb9c3L,
        0xdccd879fc967d41aL,
        0x5400e987bbc1c920L,
        0x290123e9aab23b68L,
        0xf9a0b6720aaf6521L,
        0xf808e40e8d5b3e69L,
        0xb60b1d1230b20e04L,
        0xb1c6f22b5e6f48c2L,
        0x1e38aeb6360b1af3L,
        0x25c6da63c38de1b0L,
        0x579c487e5a38ad0eL,
        0x2d835a9df0c6d851L,
        0xf8e431456cf88e65L,
        0x1b8e9ecb641b58ffL,
        0xe272467e3d222f3fL,
        0x5b0ed81dcc6abb0fL,
        0x98e947129fc2b4e9L,
        0x3f2398d747b36224L,
        0x8eec7f0d19a03aadL,
        0x1953cf68300424acL,
        0x5fa8c3423c052dd7L,
        0x3792f412cb06794dL,
        0xe2bbd88bbee40bd0L,
        0x5b6aceaeae9d0ec4L,
        0xf245825a5a445275L,
        0xeed6e2f0f0d56712L,
        0x55464dd69685606bL,
        0xaa97e14c3c26b886L,
        0xd53dd99f4b3066a8L,
        0xe546a8038efe4029L,
        0xde98520472bdd033L,
        0x963e66858f6d4440L,
        0xdde7001379a44aa8L,
        0x5560c018580d5d52L,
        0xaab8f01e6e10b4a6L,
        0xcab3961304ca70e8L,
        0x3d607b97c5fd0d22L,
        0x8cb89a7db77c506aL,
        0x77f3608e92adb242L,
        0x55f038b237591ed3L,
        0x6b6c46dec52f6688L,
        0x2323ac4b3b3da015L,
        0xabec975e0a0d081aL,
        0x96e7bd358c904a21L,
        0x7e50d64177da2e54L,
        0xdde50bd1d5d0b9e9L,
        0x955e4ec64b44e864L,
        0xbd5af13bef0b113eL,
        0xecb1ad8aeacdd58eL,
        0x67de18eda5814af2L,
        0x80eacf948770ced7L,
        0xa1258379a94d028dL,
        0x96ee45813a04330L,
        0x8bca9d6e188853fcL,
        0x775ea264cf55347dL,
        0x95364afe032a819dL,
        0x3a83ddbd83f52204L,
        0xc4926a9672793542L,
        0x75b7053c0f178293L,
        0x5324c68b12dd6338L,
        0xd3f6fc16ebca5e03L,
        0x88f4bb1ca6bcf584L,
        0x2b31e9e3d06c32e5L,
        0x3aff322e62439fcfL,
        0x9befeb9fad487c2L,
        0x4c2ebe687989a9b3L,
        0xf9d37014bf60a10L,
        0x538484c19ef38c94L,
        0x2865a5f206b06fb9L,
        0xf93f87b7442e45d3L,
        0xf78f69a51539d748L,
        0xb573440e5a884d1bL,
        0x31680a88f8953030L,
        0xfdc20d2b36ba7c3dL,
        0x3d32907604691b4cL,
        0xa63f9a49c2c1b10fL,
        0xfcf80dc33721d53L,
        0xd3c36113404ea4a8L,
        0x645a1cac083126e9L,
        0x3d70a3d70a3d70a3L,
        0xccccccccccccccccL,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x0L,
        0x4000000000000000L,
        0x5000000000000000L,
        0xa400000000000000L,
        0x4d00000000000000L,
        0xf020000000000000L,
        0x6c28000000000000L,
        0xc732000000000000L,
        0x3c7f400000000000L,
        0x4b9f100000000000L,
        0x1e86d40000000000L,
        0x1314448000000000L,
        0x17d955a000000000L,
        0x5dcfab0800000000L,
        0x5aa1cae500000000L,
        0xf14a3d9e40000000L,
        0x6d9ccd05d0000000L,
        0xe4820023a2000000L,
        0xdda2802c8a800000L,
        0xd50b2037ad200000L,
        0x4526f422cc340000L,
        0x9670b12b7f410000L,
        0x3c0cdd765f114000L,
        0xa5880a69fb6ac800L,
        0x8eea0d047a457a00L,
        0x72a4904598d6d880L,
        0x47a6da2b7f864750L,
        0x999090b65f67d924L,
        0xfff4b4e3f741cf6dL,
        0xbff8f10e7a8921a4L,
        0xaff72d52192b6a0dL,
        0x9bf4f8a69f764490L,
        0x2f236d04753d5b4L,
        0x1d762422c946590L,
        0x424d3ad2b7b97ef5L,
        0xd2e0898765a7deb2L,
        0x63cc55f49f88eb2fL,
        0x3cbf6b71c76b25fbL,
        0x8bef464e3945ef7aL,
        0x97758bf0e3cbb5acL,
        0x3d52eeed1cbea317L,
        0x4ca7aaa863ee4bddL,
        0x8fe8caa93e74ef6aL,
        0xb3e2fd538e122b44L,
        0x60dbbca87196b616L,
        0xbc8955e946fe31cdL,
        0x6babab6398bdbe41L,
        0xc696963c7eed2dd1L,
        0xfc1e1de5cf543ca2L,
        0x3b25a55f43294bcbL,
        0x49ef0eb713f39ebeL,
        0x6e3569326c784337L,
        0x49c2c37f07965404L,
        0xdc33745ec97be906L,
        0x69a028bb3ded71a3L,
        0xc40832ea0d68ce0cL,
        0xf50a3fa490c30190L,
        0x792667c6da79e0faL,
        0x577001b891185938L,
        0xed4c0226b55e6f86L,
        0x544f8158315b05b4L,
        0x696361ae3db1c721L,
        0x3bc3a19cd1e38e9L,
        0x4ab48a04065c723L,
        0x62eb0d64283f9c76L,
        0x3ba5d0bd324f8394L,
        0xca8f44ec7ee36479L,
        0x7e998b13cf4e1ecbL,
        0x9e3fedd8c321a67eL,
        0xc5cfe94ef3ea101eL,
        0xbba1f1d158724a12L,
        0x2a8a6e45ae8edc97L,
        0xf52d09d71a3293bdL,
        0x593c2626705f9c56L,
        0x6f8b2fb00c77836cL,
        0xb6dfb9c0f956447L,
        0x4724bd4189bd5eacL,
        0x58edec91ec2cb657L,
        0x2f2967b66737e3edL,
        0xbd79e0d20082ee74L,
        0xecd8590680a3aa11L,
        0xe80e6f4820cc9495L,
        0x3109058d147fdcddL,
        0xbd4b46f0599fd415L,
        0x6c9e18ac7007c91aL,
        0x3e2cf6bc604ddb0L,
        0x84db8346b786151cL,
        0xe612641865679a63L,
        0x4fcb7e8f3f60c07eL,
        0xe3be5e330f38f09dL,
        0x5cadf5bfd3072cc5L,
        0x73d9732fc7c8f7f6L,
        0x2867e7fddcdd9afaL,
        0xb281e1fd541501b8L,
        0x1f225a7ca91a4226L,
        0x3375788de9b06958L,
        0x52d6b1641c83aeL,
        0xc0678c5dbd23a49aL,
        0xf840b7ba963646e0L,
        0xb650e5a93bc3d898L,
        0xa3e51f138ab4cebeL,
        0xc66f336c36b10137L,
        0xb80b0047445d4184L,
        0xa60dc059157491e5L,
        0x87c89837ad68db2fL,
        0x29babe4598c311fbL,
        0xf4296dd6fef3d67aL,
        0x1899e4a65f58660cL,
        0x5ec05dcff72e7f8fL,
        0x76707543f4fa1f73L,
        0x6a06494a791c53a8L,
        0x487db9d17636892L,
        0x45a9d2845d3c42b6L,
        0xb8a2392ba45a9b2L,
        0x8e6cac7768d7141eL,
        0x3207d795430cd926L,
        0x7f44e6bd49e807b8L,
        0x5f16206c9c6209a6L,
        0x36dba887c37a8c0fL,
        0xc2494954da2c9789L,
        0xf2db9baa10b7bd6cL,
        0x6f92829494e5acc7L,
        0xcb772339ba1f17f9L,
        0xff2a760414536efbL,
        0xfef5138519684abaL,
        0x7eb258665fc25d69L,
        0xef2f773ffbd97a61L,
        0xaafb550ffacfd8faL,
        0x95ba2a53f983cf38L,
        0xdd945a747bf26183L,
        0x94f971119aeef9e4L,
        0x7a37cd5601aab85dL,
        0xac62e055c10ab33aL,
        0x577b986b314d6009L,
        0xed5a7e85fda0b80bL,
        0x14588f13be847307L,
        0x596eb2d8ae258fc8L,
        0x6fca5f8ed9aef3bbL,
        0x25de7bb9480d5854L,
        0xaf561aa79a10ae6aL,
        0x1b2ba1518094da04L,
        0x90fb44d2f05d0842L,
        0x353a1607ac744a53L,
        0x42889b8997915ce8L,
        0x69956135febada11L,
        0x43fab9837e699095L,
        0x94f967e45e03f4bbL,
        0x1d1be0eebac278f5L,
        0x6462d92a69731732L,
        0x7d7b8f7503cfdcfeL,
        0x5cda735244c3d43eL,
        0x3a0888136afa64a7L,
        0x88aaa1845b8fdd0L,
        0x8aad549e57273d45L,
        0x36ac54e2f678864bL,
        0x84576a1bb416a7ddL,
        0x656d44a2a11c51d5L,
        0x9f644ae5a4b1b325L,
        0x873d5d9f0dde1feeL,
        0xa90cb506d155a7eaL,
        0x9a7f12442d588f2L,
        0xc11ed6d538aeb2fL,
        0x8f1668c8a86da5faL,
        0xf96e017d694487bcL,
        0x37c981dcc395a9acL,
        0x85bbe253f47b1417L,
        0x93956d7478ccec8eL,
        0x387ac8d1970027b2L,
        0x6997b05fcc0319eL,
        0x441fece3bdf81f03L,
        0xd527e81cad7626c3L,
        0x8a71e223d8d3b074L,
        0xf6872d5667844e49L,
        0xb428f8ac016561dbL,
        0xe13336d701beba52L,
        0xecc0024661173473L,
        0x27f002d7f95d0190L,
        0x31ec038df7b441f4L,
        0x7e67047175a15271L,
        0xf0062c6e984d386L,
        0x52c07b78a3e60868L,
        0xa7709a56ccdf8a82L,
        0x88a66076400bb691L,
        0x6acff893d00ea435L,
        0x583f6b8c4124d43L,
        0xc3727a337a8b704aL,
        0x744f18c0592e4c5cL,
        0x1162def06f79df73L,
        0x8addcb5645ac2ba8L,
        0x6d953e2bd7173692L,
        0xc8fa8db6ccdd0437L,
        0x1d9c9892400a22a2L,
        0x2503beb6d00cab4bL,
        0x2e44ae64840fd61dL,
        0x5ceaecfed289e5d2L,
        0x7425a83e872c5f47L,
        0xd12f124e28f77719L,
        0x82bd6b70d99aaa6fL,
        0x636cc64d1001550bL,
        0x3c47f7e05401aa4eL,
        0x65acfaec34810a71L,
        0x7f1839a741a14d0dL,
        0x1ede48111209a050L,
        0x934aed0aab460432L,
        0xf81da84d5617853fL,
        0x36251260ab9d668eL,
        0xc1d72b7c6b426019L,
        0xb24cf65b8612f81fL,
        0xdee033f26797b627L,
        0x169840ef017da3b1L,
        0x8e1f289560ee864eL,
        0xf1a6f2bab92a27e2L,
        0xae10af696774b1dbL,
        0xacca6da1e0a8ef29L,
        0x17fd090a58d32af3L,
        0xddfc4b4cef07f5b0L,
        0x4abdaf101564f98eL,
        0x9d6d1ad41abe37f1L,
        0x84c86189216dc5edL,
        0x32fd3cf5b4e49bb4L,
        0x3fbc8c33221dc2a1L,
        0xfabaf3feaa5334aL,
        0x29cb4d87f2a7400eL,
        0x743e20e9ef511012L,
        0x914da9246b255416L,
        0x1ad089b6c2f7548eL,
        0xa184ac2473b529b1L,
        0xc9e5d72d90a2741eL,
        0x7e2fa67c7a658892L,
        0xddbb901b98feeab7L,
        0x552a74227f3ea565L,
        0xd53a88958f87275fL,
        0x8a892abaf368f137L,
        0x2d2b7569b0432d85L,
        0x9c3b29620e29fc73L,
        0x8349f3ba91b47b8fL,
        0x241c70a936219a73L,
        0xed238cd383aa0110L,
        0xf4363804324a40aaL,
        0xb143c6053edcd0d5L,
        0xdd94b7868e94050aL,
        0xca7cf2b4191c8326L,
        0xfd1c2f611f63a3f0L,
        0xbc633b39673c8cecL,
        0xd5be0503e085d813L,
        0x4b2d8644d8a74e18L,
        0xddf8e7d60ed1219eL,
        0xcabb90e5c942b503L,
        0x3d6a751f3b936243L,
        0xcc512670a783ad4L,
        0x27fb2b80668b24c5L,
        0xb1f9f660802dedf6L,
        0x5e7873f8a0396973L,
        0xdb0b487b6423e1e8L,
        0x91ce1a9a3d2cda62L,
        0x7641a140cc7810fbL,
        0xa9e904c87fcb0a9dL,
        0x546345fa9fbdcd44L,
        0xa97c177947ad4095L,
        0x49ed8eabcccc485dL,
        0x5c68f256bfff5a74L,
        0x73832eec6fff3111L,
        0xc831fd53c5ff7eabL,
        0xba3e7ca8b77f5e55L,
        0x28ce1bd2e55f35ebL,
        0x7980d163cf5b81b3L,
        0xd7e105bcc332621fL,
        0x8dd9472bf3fefaa7L,
        0xb14f98f6f0feb951L,
        0x6ed1bf9a569f33d3L,
        0xa862f80ec4700c8L,
        0xcd27bb612758c0faL,
        0x8038d51cb897789cL,
        0xe0470a63e6bd56c3L,
        0x1858ccfce06cac74L,
        0xf37801e0c43ebc8L,
        0xd30560258f54e6baL,
        0x47c6b82ef32a2069L,
        0x4cdc331d57fa5441L,
        0xe0133fe4adf8e952L,
        0x58180fddd97723a6L,
        0x570f09eaa7ea7648L };

    /**
     * Prevents instantiation.
     */
    private FastDoubleMath() {

    }

    static double decFloatLiteralToDouble(
        int index,
        boolean isNegative,
        long digits,
        int exponent,
        int virtualIndexOfPoint,
        long exp_number,
        boolean isDigitsTruncated,
        int skipCountInTruncatedDigits
    ) {
        if (digits == 0) {
            return isNegative ? -0.0 : 0.0;
        }
        final double outDouble;
        if (isDigitsTruncated) {
            final long exponentOfTruncatedDigits = virtualIndexOfPoint - index + skipCountInTruncatedDigits + exp_number;

            // We have too many digits. We may have to round up.
            // To know whether rounding up is needed, we may have to examine up to 768 digits.

            // There are cases, in which rounding has no effect.
            if (FASTFLOAT_DEC_SMALLEST_POWER <= exponentOfTruncatedDigits && exponentOfTruncatedDigits <= FASTFLOAT_DEC_LARGEST_POWER) {
                double withoutRounding = tryDecToDoubleWithFastAlgorithm(isNegative, digits, (int) exponentOfTruncatedDigits);
                double roundedUp = tryDecToDoubleWithFastAlgorithm(isNegative, digits + 1, (int) exponentOfTruncatedDigits);
                if (!Double.isNaN(withoutRounding) && Objects.equals(roundedUp, withoutRounding)) {
                    return withoutRounding;
                }
            }

            // We have to take a slow path.
            // return Double.parseDouble(str.toString());
            outDouble = Double.NaN;

        } else if (FASTFLOAT_DEC_SMALLEST_POWER <= exponent && exponent <= FASTFLOAT_DEC_LARGEST_POWER) {
            outDouble = tryDecToDoubleWithFastAlgorithm(isNegative, digits, exponent);
        } else {
            outDouble = Double.NaN;
        }
        return outDouble;
    }

    /**
     * Computes {@code uint128 product = (uint64)x * (uint64)y}.
     * <p>
     * References:
     * <dl>
     *     <dt>Getting the high part of 64 bit integer multiplication</dt>
     *     <dd><a href="https://stackoverflow.com/questions/28868367/getting-the-high-part-of-64-bit-integer-multiplication">
     *         stackoverflow</a></dd>
     * </dl>
     *
     * @param x uint64 factor x
     * @param y uint64 factor y
     * @return uint128 product of x and y
     */
    private static Value128 fullMultiplication(long x, long y) {
        long x0 = x & 0xffffffffL, x1 = x >>> 32;
        long y0 = y & 0xffffffffL, y1 = y >>> 32;
        long p11 = x1 * y1, p01 = x0 * y1;
        long p10 = x1 * y0, p00 = x0 * y0;

        // 64-bit product + two 32-bit values
        long middle = p10 + (p00 >>> 32) + (p01 & 0xffffffffL);
        return new Value128(
            // 64-bit product + two 32-bit values
            p11 + (middle >>> 32) + (p01 >>> 32),
            // Add LOW PART and lower half of MIDDLE PART
            (middle << 32) | (p00 & 0xffffffffL)
        );
    }

    static double hexFloatLiteralToDouble(
        int index,
        boolean isNegative,
        long digits,
        long exponent,
        int virtualIndexOfPoint,
        long exp_number,
        boolean isDigitsTruncated,
        int skipCountInTruncatedDigits
    ) {
        if (digits == 0) {
            return isNegative ? -0.0 : 0.0;
        }
        final double outDouble;
        if (isDigitsTruncated) {
            final long truncatedExponent = (virtualIndexOfPoint - index + skipCountInTruncatedDigits) * 4L + exp_number;

            // We have too many digits. We may have to round up.
            // To know whether rounding up is needed, we may have to examine up to 768 digits.

            // There are cases, in which rounding has no effect.
            if (FASTFLOAT_HEX_SMALLEST_POWER <= truncatedExponent && truncatedExponent <= FASTFLOAT_HEX_LARGEST_POWER) {
                double withoutRounding = tryHexToDoubleWithFastAlgorithm(isNegative, digits, (int) truncatedExponent);
                double roundedUp = tryHexToDoubleWithFastAlgorithm(isNegative, digits + 1, (int) truncatedExponent);
                if (!Double.isNaN(withoutRounding) && Objects.equals(roundedUp, withoutRounding)) {
                    return withoutRounding;
                }
            }

            // We have to take a slow path.
            outDouble = Double.NaN;

        } else if (FASTFLOAT_HEX_SMALLEST_POWER <= exponent && exponent <= FASTFLOAT_HEX_LARGEST_POWER) {
            outDouble = tryHexToDoubleWithFastAlgorithm(isNegative, digits, (int) exponent);
        } else {
            outDouble = Double.NaN;
        }
        return outDouble;
    }

    /**
     * Attempts to compute {@literal digits * 10^(power)} exactly;
     * and if "negative" is true, negate the result.
     * <p>
     * This function will only work in some cases, when it does not work it
     * returns null. This should work *most of the time* (like 99% of the time).
     * We assume that power is in the [FASTFLOAT_SMALLEST_POWER,
     * FASTFLOAT_LARGEST_POWER] interval: the caller is responsible for this check.
     *
     * @param isNegative whether the number is negative
     * @param digits     uint64 the digits of the number
     * @param power      int32 the exponent of the number
     * @return the computed double on success, {@link Double#NaN} on failure
     */
    static double tryDecToDoubleWithFastAlgorithm(boolean isNegative, long digits, int power) {
        if (digits == 0 || power < -380 - 19) {
            return isNegative ? -0.0 : 0.0;
        }
        if (power > 380) {
            return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }

        // we start with a fast path
        // It was described in
        // Clinger WD. How to read floating point numbers accurately.
        // ACM SIGPLAN Notices. 1990
        if (-22 <= power && power <= 22 && Long.compareUnsigned(digits, 0x1fffffffffffffL) <= 0) {
            // convert the integer into a double. This is lossless since
            // 0 <= i <= 2^53 - 1.
            double d = (double) digits;
            //
            // The general idea is as follows.
            // If 0 <= s < 2^53 and if 10^0 <= p <= 10^22 then
            // 1) Both s and p can be represented exactly as 64-bit floating-point
            // values (binary64).
            // 2) Because s and p can be represented exactly as floating-point values,
            // then s * p and s / p will produce correctly rounded values.
            //
            if (power < 0) {
                d = d / powerOfTen[-power];
            } else {
                d = d * powerOfTen[power];
            }
            return (isNegative) ? -d : d;
        }

        // The fast path has now failed, so we are falling back on the slower path.

        // We are going to need to do some 64-bit arithmetic to get a more precise product.
        // We use a table lookup approach.
        // It is safe because
        // power >= FASTFLOAT_SMALLEST_POWER
        // and power <= FASTFLOAT_LARGEST_POWER
        // We recover the mantissa of the power, it has a leading 1. It is always
        // rounded down.
        long factor_mantissa = MANTISSA_64[power - FASTFLOAT_DEC_SMALLEST_POWER];

        // The exponent is 1024 + 63 + power
        // + floor(log(5**power)/log(2)).
        // The 1024 comes from the ieee64 standard.
        // The 63 comes from the fact that we use a 64-bit word.
        //
        // Computing floor(log(5**power)/log(2)) could be
        // slow. Instead we use a fast function.
        //
        // For power in (-400,350), we have that
        // (((152170 + 65536) * power ) >> 16);
        // is equal to
        // floor(log(5**power)/log(2)) + power when power >= 0
        // and it is equal to
        // ceil(log(5**-power)/log(2)) + power when power < 0
        //
        //
        // The 65536 is (1<<16) and corresponds to
        // (65536 * power) >> 16 ---> power
        //
        // ((152170 * power ) >> 16) is equal to
        // floor(log(5**power)/log(2))
        //
        // Note that this is not magic: 152170/(1<<16) is
        // approximately equal to log(5)/log(2).
        // The 1<<16 value is a power of two; we could use a
        // larger power of 2 if we wanted to.
        //
        long exponent = (((152170 + 65536) * power) >> 16) + 1024 + 63;
        // We want the most significant bit of digits to be 1. Shift if needed.
        int lz = Long.numberOfLeadingZeros(digits);
        digits <<= lz;
        // We want the most significant 64 bits of the product. We know
        // this will be non-zero because the most significant bit of i is
        // 1.
        Value128 product = fullMultiplication(digits, factor_mantissa);
        long lower = product.low;
        long upper = product.high;
        // We know that upper has at most one leading zero because
        // both i and factor_mantissa have a leading one. This means
        // that the result is at least as large as ((1<<63)*(1<<63))/(1<<64).

        // As long as the first 9 bits of "upper" are not "1", then we
        // know that we have an exact computed value for the leading
        // 55 bits because any imprecision would play out as a +1, in
        // the worst case.
        // Having 55 bits is necessary because
        // we need 53 bits for the mantissa but we have to have one rounding bit and
        // we can waste a bit if the most significant bit of the product is zero.
        // We expect this next branch to be rarely taken (say 1% of the time).
        // When (upper & 0x1FF) == 0x1FF, it can be common for
        // lower + i < lower to be true (proba. much higher than 1%).
        if ((upper & 0x1FF) == 0x1FF && Long.compareUnsigned(lower + digits, lower) < 0) {
            long factor_mantissa_low = MANTISSA_128[power - FASTFLOAT_DEC_SMALLEST_POWER];
            // next, we compute the 64-bit x 128-bit multiplication, getting a 192-bit
            // result (three 64-bit values)
            product = fullMultiplication(digits, factor_mantissa_low);
            long product_low = product.low;
            long product_middle2 = product.high;
            long product_middle1 = lower;
            long product_high = upper;
            long product_middle = product_middle1 + product_middle2;
            if (Long.compareUnsigned(product_middle, product_middle1) < 0) {
                product_high++; // overflow carry
            }

            // we want to check whether mantissa *i + i would affect our result
            // This does happen, e.g. with 7.3177701707893310e+15
            if (((product_middle + 1 == 0)
                && ((product_high & 0x1ff) == 0x1ff)
                && (product_low + Long.compareUnsigned(digits, product_low) < 0))) { // let us be prudent and bail out.
                return Double.NaN;
            }
            upper = product_high;
            // lower = product_middle;
        }

        // The final mantissa should be 53 bits with a leading 1.
        // We shift it so that it occupies 54 bits with a leading 1.
        long upperbit = upper >>> 63;
        long mantissa = upper >>> (upperbit + 9);
        lz += (int) (1 ^ upperbit);
        // Here we have mantissa < (1<<54).

        // We have to round to even. The "to even" part
        // is only a problem when we are right in between two floats
        // which we guard against.
        // If we have lots of trailing zeros, we may fall right between two
        // floating-point values.
        if (((upper & 0x1ff) == 0x1ff) || ((upper & 0x1ff) == 0) && (mantissa & 3) == 1) {
            // if mantissa & 1 == 1 we might need to round up.
            //
            // Scenarios:
            // 1. We are not in the middle. Then we should round up.
            //
            // 2. We are right in the middle. Whether we round up depends
            // on the last significant bit: if it is "one" then we round
            // up (round to even) otherwise, we do not.
            //
            // So if the last significant bit is 1, we can safely round up.
            // Hence we only need to bail out if (mantissa & 3) == 1.
            // Otherwise we may need more accuracy or analysis to determine whether
            // we are exactly between two floating-point numbers.
            // It can be triggered with 1e23.
            // Note: because the factor_mantissa and factor_mantissa_low are
            // almost always rounded down (except for small positive powers),
            // almost always should round up.
            return Double.NaN;
        }

        mantissa += 1;
        mantissa >>>= 1;

        // Here we have mantissa < (1<<53), unless there was an overflow
        if (mantissa >= (1L << 53)) {
            // This will happen when parsing values such as 7.2057594037927933e+16
            mantissa = (1L << 52);
            lz--; // undo previous addition
        }

        mantissa &= ~(1L << 52);
        long real_exponent = exponent - lz;
        // we have to check that real_exponent is in range, otherwise we bail out
        if ((real_exponent < 1) || (real_exponent > 2046)) {
            return Double.NaN;
        }

        long bits = mantissa | real_exponent << 52 | (isNegative ? 1L << 63 : 0L);
        return Double.longBitsToDouble(bits);
    }

    /**
     * Attempts to compute {@literal digits * 2^(power)} exactly;
     * and if "negative" is true, negate the result.
     * <p>
     * This function will only work in some cases, when it does not work it
     * returns null.
     *
     * @param isNegative whether the number is negative
     * @param digits     uint64 the digits of the number
     * @param power      int32 the exponent of the number
     * @return the computed double on success, null on failure
     */
    static double tryHexToDoubleWithFastAlgorithm(boolean isNegative, long digits, int power) {
        if (digits == 0 || power < Double.MIN_EXPONENT - 54) {
            return isNegative ? -0.0 : 0.0;
        }
        if (power > Double.MAX_EXPONENT) {
            return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }

        // we start with a fast path
        // We try to mimic the fast described by Clinger WD for decimal
        // float number literals. How to read floating point numbers accurately.
        // ACM SIGPLAN Notices. 1990
        if (Long.compareUnsigned(digits, 0x1fffffffffffffL) <= 0) {
            // convert the integer into a double. This is lossless since
            // 0 <= i <= 2^53 - 1.
            double d = (double) digits;
            //
            // The general idea is as follows.
            // If 0 <= s < 2^53 then
            // 1) Both s and p can be represented exactly as 64-bit floating-point
            // values (binary64).
            // 2) Because s and p can be represented exactly as floating-point values,
            // then s * p will produce correctly rounded values.
            //
            d = d * Math.scalb(1d, power);
            if (isNegative) {
                d = -d;
            }
            return d;
        }

        // The fast path has failed
        return Double.NaN;
    }

    private static class Value128 {

        final long high, low;

        private Value128(long high, long low) {
            this.high = high;
            this.low = low;
        }
    }
}
