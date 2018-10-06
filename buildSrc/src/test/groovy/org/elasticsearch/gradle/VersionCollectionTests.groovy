package org.elasticsearch.gradle

import org.elasticsearch.gradle.test.GradleUnitTestCase
import org.junit.Test

class VersionCollectionTests extends GradleUnitTestCase {

  String formatVersion(String version) {
    return " public static final Version V_${version.replaceAll("\\.", "_")} "
  }
  List<String> allVersions = [formatVersion('5.0.0'), formatVersion('5.0.0_alpha1'), formatVersion('5.0.0_alpha2'), formatVersion('5.0.0_beta1'),
                     formatVersion('5.0.0_rc1'),formatVersion('5.0.0_rc2'),formatVersion('5.0.1'), formatVersion('5.0.2'),
                     formatVersion('5.1.1'), formatVersion('5.1.2'), formatVersion('5.2.0'), formatVersion('5.2.1'), formatVersion('6.0.0'),
                     formatVersion('6.0.1'), formatVersion('6.1.0'), formatVersion('6.1.1'), formatVersion('6.2.0'), formatVersion('6.3.0'),
                     formatVersion('7.0.0_alpha1'), formatVersion('7.0.0_alpha2')]

  /**
   * This validates the logic of being on a unreleased major branch with a staged major-1.minor sibling. This case happens when a version is
   * branched from Major-1.x At the time of this writing 6.2 is unreleased and 6.3 is the 6.x branch. This test simulates the behavior
   * from 7.0 perspective, or master at the time of this writing.
   */
  @Test
  void testAgainstMajorUnreleasedWithExistingStagedMinorRelease() {
    VersionCollection vc = new VersionCollection(allVersions)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, Version.fromString("6.3.0-SNAPSHOT"))
    assertEquals(vc.stagedMinorSnapshot, Version.fromString("6.2.0-SNAPSHOT"))
    assertEquals(vc.nextBugfixSnapshot, Version.fromString("6.1.1-SNAPSHOT"))
    assertNull(vc.maintenanceBugfixSnapshot)

    vc.indexCompatible.containsAll(vc.versions)

    // This should contain the same list sans the current version
    List indexCompatList =  [Version.fromString("6.0.0"), Version.fromString("6.0.1"),
                             Version.fromString("6.1.0"), Version.fromString("6.1.1-SNAPSHOT"),
                             Version.fromString("6.2.0-SNAPSHOT"), Version.fromString("6.3.0-SNAPSHOT")]
    assertTrue(indexCompatList.containsAll(vc.indexCompatible))
    assertTrue(vc.indexCompatible.containsAll(indexCompatList))

    List wireCompatList = [Version.fromString("6.3.0-SNAPSHOT")]
    assertTrue(wireCompatList.containsAll(vc.wireCompatible))
    assertTrue(vc.wireCompatible.containsAll(wireCompatList))

    assertEquals(vc.snapshotsIndexCompatible.size(), 3)
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.3.0-SNAPSHOT")))
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.2.0-SNAPSHOT")))
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.1.1-SNAPSHOT")))

    assertEquals(vc.snapshotsWireCompatible.size(), 1)
    assertEquals(vc.snapshotsWireCompatible.first(), Version.fromString("6.3.0-SNAPSHOT"))
  }

  /**
   * This validates the logic of being on a unreleased major branch without a staged major-1.minor sibling. This case happens once a staged,
   * unreleased minor is released. At the time of this writing 6.2 is unreleased, so adding a 6.2.1 simulates a 6.2 release. This test
   * simulates the behavior from 7.0 perspective, or master at the time of this writing.
   */
  @Test
  void testAgainstMajorUnreleasedWithoutStagedMinorRelease() {
    List localVersion = allVersions.clone()
    localVersion.add(formatVersion('6.2.1')) // release 6.2

    VersionCollection vc = new VersionCollection(localVersion)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, Version.fromString("6.3.0-SNAPSHOT"))
    assertEquals(vc.stagedMinorSnapshot, null)
    assertEquals(vc.nextBugfixSnapshot, Version.fromString("6.2.1-SNAPSHOT"))
    assertNull(vc.maintenanceBugfixSnapshot)

    vc.indexCompatible.containsAll(vc.versions)

    // This should contain the same list sans the current version
    List indexCompatList =  [Version.fromString("6.0.0"), Version.fromString("6.0.1"),
                             Version.fromString("6.1.0"), Version.fromString("6.1.1"),
                             Version.fromString("6.2.0"), Version.fromString("6.2.1-SNAPSHOT"),
                             Version.fromString("6.3.0-SNAPSHOT")]
    assertTrue(indexCompatList.containsAll(vc.indexCompatible))
    assertTrue(vc.indexCompatible.containsAll(indexCompatList))

    List wireCompatList = [Version.fromString("6.3.0-SNAPSHOT")]
    assertTrue(wireCompatList.containsAll(vc.wireCompatible))
    assertTrue(vc.wireCompatible.containsAll(wireCompatList))

    assertEquals(vc.snapshotsIndexCompatible.size(), 2)
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.3.0-SNAPSHOT")))
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.2.1-SNAPSHOT")))

    assertEquals(vc.snapshotsWireCompatible.size(), 1)
    assertEquals(vc.snapshotsWireCompatible.first(), Version.fromString("6.3.0-SNAPSHOT"))
  }

  /**
   * This validates the logic of being on a unreleased minor branch with a staged minor sibling. This case happens when a version is
   * branched from Major.x At the time of this writing 6.2 is unreleased and 6.3 is the 6.x branch. This test simulates the behavior
   * from 6.3 perspective.
   */
  @Test
  void testAgainstMinorReleasedBranch() {
    List localVersion = allVersions.clone()
    localVersion.removeAll { it.toString().contains('7_0_0')} // remove all the 7.x so that the actual version is 6.3 (6.x)
    VersionCollection vc = new VersionCollection(localVersion)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, null)
    assertEquals(vc.stagedMinorSnapshot, Version.fromString("6.2.0-SNAPSHOT"))
    assertEquals(vc.nextBugfixSnapshot, Version.fromString("6.1.1-SNAPSHOT"))
    assertEquals(vc.maintenanceBugfixSnapshot, Version.fromString("5.2.1-SNAPSHOT"))

    // This should contain the same list sans the current version
    List indexCompatList = vc.versions.subList(0, vc.versions.size() - 1)
    assertTrue(indexCompatList.containsAll(vc.indexCompatible))
    assertTrue(vc.indexCompatible.containsAll(indexCompatList))

    List wireCompatList = [Version.fromString("5.2.0"), Version.fromString("5.2.1-SNAPSHOT"), Version.fromString("6.0.0"),
                           Version.fromString("6.0.1"), Version.fromString("6.1.0"), Version.fromString("6.1.1-SNAPSHOT"),
                           Version.fromString("6.2.0-SNAPSHOT")]
    assertTrue(wireCompatList.containsAll(vc.wireCompatible))
    assertTrue(vc.wireCompatible.containsAll(wireCompatList))

    assertEquals(vc.snapshotsIndexCompatible.size(), 3)
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.2.0-SNAPSHOT")))
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.1.1-SNAPSHOT")))
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("5.2.1-SNAPSHOT")))

    assertEquals(vc.snapshotsWireCompatible.size(), 3)
    assertTrue(vc.snapshotsWireCompatible.contains(Version.fromString("6.2.0-SNAPSHOT")))
    assertTrue(vc.snapshotsWireCompatible.contains(Version.fromString("6.1.1-SNAPSHOT")))
    assertTrue(vc.snapshotsWireCompatible.contains(Version.fromString("5.2.1-SNAPSHOT")))
  }

  /**
   * This validates the logic of being on a unreleased minor branch without a staged minor sibling. This case happens once a staged,
   * unreleased minor is released. At the time of this writing 6.2 is unreleased, so adding a 6.2.1 simulates a 6.2 release. This test
   * simulates the behavior from 6.3 perspective.
   */
  @Test
  void testAgainstMinorReleasedBranchNoStagedMinor() {
    List localVersion = allVersions.clone()
    // remove all the 7.x and add a 6.2.1 which means 6.2 was released
    localVersion.removeAll { it.toString().contains('7_0_0')}
    localVersion.add(formatVersion('6.2.1'))
    VersionCollection vc = new VersionCollection(localVersion)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, null)
    assertEquals(vc.stagedMinorSnapshot, null)
    assertEquals(vc.nextBugfixSnapshot, Version.fromString("6.2.1-SNAPSHOT"))
    assertEquals(vc.maintenanceBugfixSnapshot, Version.fromString("5.2.1-SNAPSHOT"))

    // This should contain the same list sans the current version
    List indexCompatList = vc.versions.subList(0, vc.versions.size() - 1)
    assertTrue(indexCompatList.containsAll(vc.indexCompatible))
    assertTrue(vc.indexCompatible.containsAll(indexCompatList))

    List wireCompatList = [Version.fromString("5.2.0"), Version.fromString("5.2.1-SNAPSHOT"), Version.fromString("6.0.0"),
                           Version.fromString("6.0.1"), Version.fromString("6.1.0"), Version.fromString("6.1.1"),
                           Version.fromString("6.2.0"), Version.fromString("6.2.1-SNAPSHOT")]
    assertTrue(wireCompatList.containsAll(vc.wireCompatible))
    assertTrue(vc.wireCompatible.containsAll(wireCompatList))

    assertEquals(vc.snapshotsIndexCompatible.size(), 2)
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("6.2.1-SNAPSHOT")))
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("5.2.1-SNAPSHOT")))

    assertEquals(vc.snapshotsWireCompatible.size(), 2)
    assertTrue(vc.snapshotsWireCompatible.contains(Version.fromString("6.2.1-SNAPSHOT")))
    assertTrue(vc.snapshotsWireCompatible.contains(Version.fromString("5.2.1-SNAPSHOT")))
  }

  /**
   * This validates the logic of being on a released minor branch. At the time of writing, 6.2 is unreleased, so this is equivalent of being
   * on 6.1.
   */
  @Test
  void testAgainstOldMinor() {

    List localVersion = allVersions.clone()
    // remove the 7 alphas and the ones greater than 6.1
    localVersion.removeAll { it.toString().contains('7_0_0') || it.toString().contains('V_6_2') || it.toString().contains('V_6_3') }
    VersionCollection vc = new VersionCollection(localVersion)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, null)
    assertEquals(vc.stagedMinorSnapshot, null)
    assertEquals(vc.nextBugfixSnapshot, null)
    assertEquals(vc.maintenanceBugfixSnapshot, Version.fromString("5.2.1-SNAPSHOT"))

    // This should contain the same list sans the current version
    List indexCompatList = vc.versions.subList(0, vc.versions.size() - 1)
    assertTrue(indexCompatList.containsAll(vc.indexCompatible))
    assertTrue(vc.indexCompatible.containsAll(indexCompatList))

    List wireCompatList = [Version.fromString("5.2.0"), Version.fromString("5.2.1-SNAPSHOT"), Version.fromString("6.0.0"),
                           Version.fromString("6.0.1"), Version.fromString("6.1.0")]
    assertTrue(wireCompatList.containsAll(vc.wireCompatible))
    assertTrue(vc.wireCompatible.containsAll(wireCompatList))

    assertEquals(vc.snapshotsIndexCompatible.size(), 1)
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("5.2.1-SNAPSHOT")))

    assertEquals(vc.snapshotsWireCompatible.size(), 1)
    assertTrue(vc.snapshotsWireCompatible.contains(Version.fromString("5.2.1-SNAPSHOT")))
  }

  /**
   * This validates the lower bound of wire compat, which is 5.0. It also validates that the span of 2.x to 5.x if it is decided to port
   * this fix all the way to the maint 5.6 release.
   */
  @Test
  void testFloorOfWireCompatVersions() {
    List localVersion = [formatVersion('2.0.0'), formatVersion('2.0.1'), formatVersion('2.1.0'), formatVersion('2.1.1'),
                          formatVersion('5.0.0'), formatVersion('5.0.1'), formatVersion('5.1.0'), formatVersion('5.1.1'),
                          formatVersion('5.2.0'),formatVersion('5.2.1'),formatVersion('5.3.0'),formatVersion('5.3.1'),
                          formatVersion('5.3.2')]
    VersionCollection vc = new VersionCollection(localVersion)
    assertNotNull(vc)
    assertEquals(vc.maintenanceBugfixSnapshot, Version.fromString("2.1.1-SNAPSHOT"))

    // This should contain the same list sans the current version
    List indexCompatList = vc.versions.subList(0, vc.versions.size() - 1)
    assertTrue(indexCompatList.containsAll(vc.indexCompatible))
    assertTrue(vc.indexCompatible.containsAll(indexCompatList))

    List wireCompatList = [Version.fromString("2.1.0"), Version.fromString("2.1.1-SNAPSHOT"), Version.fromString("5.0.0"),
                           Version.fromString("5.0.1"), Version.fromString("5.1.0"),
                           Version.fromString("5.1.1"), Version.fromString("5.2.0"), Version.fromString("5.2.1"),
                           Version.fromString("5.3.0"), Version.fromString("5.3.1")]

    List<Version> compatible = vc.wireCompatible
    assertTrue(wireCompatList.containsAll(compatible))
    assertTrue(vc.wireCompatible.containsAll(wireCompatList))

    assertEquals(vc.snapshotsIndexCompatible.size(), 1)
    assertTrue(vc.snapshotsIndexCompatible.contains(Version.fromString("2.1.1-SNAPSHOT")))

    // ensure none of the 2.x snapshots appear here, as this is the floor of bwc for wire compat
    assertEquals(vc.snapshotsWireCompatible.size(), 0)
  }
}
