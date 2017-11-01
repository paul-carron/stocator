package com.ibm.stocator.fs.cos.systemtests;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.stocator.fs.cos.COSTestConstants;
import com.ibm.stocator.fs.cos.systemtests.COSTestUtils;

/**
 * Test deletion operations
 */
public class TestCOSFileSystemDelete extends COSFileSystemBaseTest {

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testDeleteEmptyFile() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteEmptyFile");
    createEmptyFile(file);
    COSTestUtils.noteAction("about to delete");
    assertDeleted(file, true);
  }

  @Ignore("Unexpected")
  public void testDeleteEmptyFileTwice() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteEmptyFileTwice");
    createEmptyFile(file);
    assertDeleted(file, true);
    COSTestUtils.noteAction("multiple creates, and deletes");
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
    createEmptyFile(file);
    assertDeleted(file, true);
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testDeleteNonEmptyFile() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteNonEmptyFile");
    createFile(file);
    assertDeleted(file, true);
  }

  @Ignore("Unexpected")
  public void testDeleteNonEmptyFileTwice() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteNonEmptyFileTwice");
    createFile(file);
    assertDeleted(file, true);
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
    createFile(file);
    assertDeleted(file, true);
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testDeleteTestDir() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/");
    sFileSystem.delete(file, true);
    assertPathDoesNotExist("Test dir found", file);
  }

  /**
   * Test recursive root directory deletion fails if there is an entry underneath
   * @throws Throwable
   */
  @Ignore("Not supported")
  public void testRmRootDirRecursiveIsForbidden() throws Throwable {
    Path root = path(getBaseURI() + "/");
    Path testFile = path(getBaseURI() + "/test");
    createFile(testFile);
    assertTrue("rm(/) returned false", sFileSystem.delete(root, true));
    assertExists("Root dir is missing", root);
    assertPathDoesNotExist("test file not deleted", testFile);
  }

}
