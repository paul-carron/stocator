package com.ibm.stocator.fs.cos.systemtests;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.stocator.fs.cos.COSTestConstants;

import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.assertListStatusFinds;
import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.cleanup;
import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.dumpStats;
import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.touch;

public class TestCOSFileSystemLsOperations extends COSFileSystemBaseTest {

  private static Path[] sTestDirs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    createCOSFileSystem();
    if (sFileSystem != null) {
      createTestSubdirs();
    }
  }

  /**
   * Create subdirectories and files under test/ for those tests
   * that want them. Doing so adds overhead to setup and teardown,
   * so should only be done for those tests that need them.
   * @throws IOException on an IO problem
   */
  private static void createTestSubdirs() throws IOException {

    sTestDirs = new Path[]{ new Path(sBaseURI + "/test/cos/a"),
                            new Path(sBaseURI + "/test/cos/b"),
                            new Path(sBaseURI + "/test/cos/c/1")};
    for (Path path : sTestDirs) {
      createEmptyFile(path);
    }
  }

  @Ignore("Not supported")
  public void testListLevelTest() throws Exception {
    FileStatus[] paths = sFileSystem.listStatus(path(getBaseURI() + "/test"));
    assertEquals(dumpStats(getBaseURI() + "/test", paths), 1, paths.length);
    assertEquals(path(getBaseURI() + "/test/cos"), paths[0].getPath());
  }

  @Ignore("Not supported")
  public void testListLevelTestCOS() throws Exception {
    FileStatus[] paths;
    paths = sFileSystem.listStatus(path(getBaseURI() + "/test/cos"));
    String stats = dumpStats("/test/cos", paths);
    assertEquals("Paths.length wrong in " + stats, 3, paths.length);
    assertEquals("Path element[0] wrong: " + stats, path(getBaseURI() + "/test/cos/a"),
                 paths[0].getPath());
    assertEquals("Path element[1] wrong: " + stats, path(getBaseURI() + "/test/cos/b"),
                 paths[1].getPath());
    assertEquals("Path element[2] wrong: " + stats, path(getBaseURI() + "/test/cos/c"),
                 paths[2].getPath());
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testListStatusEmptyDirectory() throws Exception {
    FileStatus[] paths;
    paths = sFileSystem.listStatus(path(getBaseURI() + "/test/cos/a"));
    assertEquals(dumpStats("/test/cos/a", paths), 0,
                 paths.length);
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testListStatusFile() throws Exception {
    describe("Create a single file under /test;"
             + " assert that listStatus(/test) finds it");
    Path file = path(getBaseURI() + "/test/filename");
    createFile(file);
    FileStatus[] pathStats = sFileSystem.listStatus(file);
    assertEquals(dumpStats("/test/", pathStats),
                 1,
                 pathStats.length);
    //and assert that the len of that ls'd path is the same as the original
    FileStatus lsStat = pathStats[0];
    assertEquals("Wrong file len in listing of " + lsStat,
        data.length, lsStat.getLen());
  }

  public void testListEmptyRoot() throws Throwable {
    describe("Empty the root dir and verify that an LS / returns {}");
    cleanup("testListEmptyRoot", sFileSystem, "/test");
    cleanup("testListEmptyRoot", sFileSystem, "/user");
    FileStatus[] fileStatuses = sFileSystem.listStatus(path(getBaseURI() + "/"));
    assertEquals("Non-empty root" + dumpStats("/", fileStatuses),
                 0,
                 fileStatuses.length);
  }

  @Ignore("Unexpected")
  public void testListNonEmptyRoot() throws Throwable {
    Path test = path(getBaseURI() + "/test");
    touch(sFileSystem, test);
    FileStatus[] fileStatuses = sFileSystem.listStatus(path(getBaseURI() + "/"));
    String stats = dumpStats("/", fileStatuses);
    assertEquals("Wrong #of root children" + stats, 1, fileStatuses.length);
    FileStatus status = fileStatuses[0];
    assertEquals("Wrong path value" + stats,test, status.getPath());
  }

  @Ignore("Not Supported")
  public void testListStatusRootDir() throws Throwable {
    Path dir = path(getBaseURI() + "/");
    Path child = path(getBaseURI() + "/test");
    touch(sFileSystem, child);
    assertListStatusFinds(sFileSystem, dir, child);
  }

  @Ignore("Not supported")
  public void testListStatusFiltered() throws Throwable {
    Path dir = path(getBaseURI() + "/");
    Path child = path(getBaseURI() + "/test");
    touch(sFileSystem, child);
    FileStatus[] stats = sFileSystem.listStatus(dir, new AcceptAllFilter());
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(child)) {
        found = true;
      }
    }
    assertTrue("Path " + child
                      + " not found in directory " + dir + ":" + builder,
                      found);
  }

  /**
   * A path filter that accepts everything
   */
  private class AcceptAllFilter implements PathFilter {
    @Override
    public boolean accept(Path file) {
      return true;
    }
  }

}
