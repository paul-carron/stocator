package com.ibm.stocator.fs.cos.systemtests;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.stocator.fs.ObjectStoreFileSystem;
import com.ibm.stocator.fs.cos.COSTestConstants;

import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.assertFileHasLength;
import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.readBytesToString;
import static com.ibm.stocator.fs.cos.systemtests.COSTestUtils.writeTextFile;

/**
 * Test basic filesystem operations.
 * -this is a JUnit4 test suite used to initially test the COS
 * component. Once written, there's no reason not to retain these tests.
 */

public class TestCOSFileSystemBasicOps extends COSFileSystemBaseTest {

  private static final Log LOG =
          LogFactory.getLog(TestCOSFileSystemBasicOps.class);

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testLsRoot() throws Throwable {
    Path path = new Path(getBaseURI() + "/");
    FileStatus[] statuses = sFileSystem.listStatus(path);
    assertNotNull(statuses);
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testMkDir() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/MkDir/_temporary/0");
    assertTrue(sFileSystem.mkdirs(path));
    sFileSystem.delete(new Path(getBaseURI() + "/test/MkDir"), true);
  }

  @Ignore("Unexpected")
  public void testDeleteNonexistentFile() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/DeleteNonexistentFile");
    assertFalse("delete returned true", sFileSystem.delete(path, false));
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testPutFile() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/PutFile");
    writeTextFile(sFileSystem, path, "Testing a put to a file", false);
    assertDeleted(path, false);
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testPutGetFile() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/PutGetFile");
    try {
      String text = "Testing a put and get to a file "
              + System.currentTimeMillis();
      writeTextFile(sFileSystem, path, text, false);

      String result = readBytesToString(sFileSystem, path, text.length());
      assertEquals(text, result);
    } finally {
      delete(sFileSystem, path);
    }
  }

  private void delete(ObjectStoreFileSystem fs, Path path) {
    try {
      if (!fs.delete(path, false)) {
        LOG.warn("Failed to delete " + path);
      }
    } catch (IOException e) {
      LOG.warn("deleting " + path, e);
    }
  }

  private void deleteR(ObjectStoreFileSystem fs, Path path) {
    try {
      if (!fs.delete(path, true)) {
        LOG.warn("Failed to delete " + path);
      }
    } catch (IOException e) {
      LOG.warn("deleting " + path, e);
    }
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testOverwrite() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/Overwrite");
    try {
      String text = "Testing a put to a file "
              + System.currentTimeMillis();
      writeTextFile(sFileSystem, path, text, false);
      assertFileHasLength(sFileSystem, path, text.length());
      String text2 = "Overwriting a file "
              + System.currentTimeMillis();
      writeTextFile(sFileSystem, path, text2, true);
      assertFileHasLength(sFileSystem, path, text2.length());
      String result = readBytesToString(sFileSystem, path, text2.length());
      assertEquals(text2, result);
    } finally {
      delete(sFileSystem, path);
    }
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testOverwriteDirectory() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/testOverwriteDirectory");
    try {
      sFileSystem.mkdirs(path.getParent());
      String text = "Testing a put to a file "
              + System.currentTimeMillis();
      writeTextFile(sFileSystem, path, text, false);
      assertFileHasLength(sFileSystem, path, text.length());
    } finally {
      delete(sFileSystem, path);
    }
  }

  @Test
  public void testObjectNamesWithSpaces() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/object name");
    try {
      String text = "Testing PUT and GET on object name with spaces "
              + System.currentTimeMillis();
      writeTextFile(sFileSystem, path, text, false);
      String result = readBytesToString(sFileSystem, path, text.length());
      assertEquals(text, result);
    } finally {
      delete(sFileSystem, path);
    }
  }

  @Ignore("Unexpected")
  public void testLongObjectNamesForbidden() throws Throwable {
    StringBuilder buffer = new StringBuilder(1200);
    buffer.append("/");
    for (int i = 0; i < (1200 / 4); i++) {
      buffer.append(String.format("%04x", i));
    }
    String pathString = buffer.toString();
    pathString = "/A/B/C";
    Path path = new Path(getBaseURI() + pathString);
    try {
      writeTextFile(sFileSystem, path, pathString, true);
      //if we get here, problems.
      sFileSystem.delete(path, false);
      fail("Managed to create an object with a name of length "
              + pathString.length());
    } catch (Exception e) {
      //expected
      //LOG.debug("Caught exception " + e, e);
    }
  }

  //@Test(expected = FileNotFoundException.class)
  @Ignore("Unexpected")
  public void testLsNonExistentFile() throws Exception {
    try {
      Path path = new Path(getBaseURI() + "/test/hadoop/file");
      FileStatus[] statuses = sFileSystem.listStatus(path);
      fail("Should throw FileNotFoundException on " + path
              + " but got list of length " + statuses.length);
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

}
