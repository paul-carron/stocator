package com.ibm.stocator.fs.cos.systemtests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.stocator.fs.ObjectStoreVisitor;
import com.ibm.stocator.fs.cos.COSConstants;
import com.ibm.stocator.fs.cos.COSTestConstants;

public class TestCOSFileSystemExtendedContract extends COSFileSystemBaseTest {

  private static final String BASE_URI_PROPERTY = "fs.cos.test.uri";

  @Ignore("Unexpected")
  public void testOpenNonExistingFile() throws IOException {
    final Path p = new Path("/test/testOpenNonExistingFile");
    //open it as a file, should get FileNotFoundException
    try {
      final FSDataInputStream in = sFileSystem.open(p);
      in.close();
      fail("didn't expect to get here");
    } catch (FileNotFoundException fnfe) {
      LOG.debug("Expected: " + fnfe, fnfe);
    }
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testFilesystemHasURI() throws Throwable {
    assertNotNull(sFileSystem.getUri());
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testCreateFile() throws Exception {
    final Path f = new Path(getBaseURI() + "/test/testCreateFile");
    final FSDataOutputStream fsDataOutputStream = sFileSystem.create(f);
    fsDataOutputStream.close();
    assertExists("created file", f);
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testWriteReadFile() throws Exception {
    final Path f = new Path(getBaseURI() + "/test/test");
    final FSDataOutputStream fsDataOutputStream = sFileSystem.create(f);
    final String message = "Test string";
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();
    assertExists("created file", f);
    FSDataInputStream open = null;
    try {
      open = sFileSystem.open(f);
      final byte[] bytes = new byte[512];
      final int read = open.read(bytes);
      final byte[] buffer = new byte[read];
      System.arraycopy(bytes, 0, buffer, 0, read);
      assertEquals(message, new String(buffer));
    } finally {
      sFileSystem.delete(f, false);
      IOUtils.closeStream(open);
    }
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testConfDefinesFilesystem() throws Throwable {
    Configuration conf = new Configuration();
    sBaseURI = conf.get(BASE_URI_PROPERTY);
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testConfIsValid() throws Throwable {
    Configuration conf = new Configuration();
    sBaseURI = conf.get(BASE_URI_PROPERTY);
    ObjectStoreVisitor.getStoreClient(new URI(sBaseURI), conf);
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testGetSchemeImplemented() throws Throwable {
    String scheme = sFileSystem.getScheme();
    assertEquals(COSConstants.COS,scheme);
  }

  /**
   * Assert that a filesystem is case sensitive.
   * This is done by creating a mixed-case filename and asserting that
   * its lower case version is not there.
   *
   * @throws Exception failures
   */
  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testFilesystemIsCaseSensitive() throws Exception {
    String mixedCaseFilename = "/test/UPPER.TXT";
    Path upper = path(getBaseURI() + mixedCaseFilename);
    Path lower = path(getBaseURI() + mixedCaseFilename.toLowerCase());
    assertFalse("File exists" + upper, sFileSystem.exists(upper));
    assertFalse("File exists" + lower, sFileSystem.exists(lower));
    FSDataOutputStream out = sFileSystem.create(upper);
    out.writeUTF("UPPER");
    out.close();
    FileStatus upperStatus = sFileSystem.getFileStatus(upper);
    assertExists("Original upper case file" + upper, upper);
    //verify the lower-case version of the filename doesn't exist
    assertPathDoesNotExist("lower case file", lower);
    //now overwrite the lower case version of the filename with a
    //new version.
    out = sFileSystem.create(lower);
    out.writeUTF("l");
    out.close();
    assertExists("lower case file", lower);
    //verify the length of the upper file hasn't changed
    assertExists("Original upper case file " + upper, upper);
    FileStatus newStatus = sFileSystem.getFileStatus(upper);
    assertEquals("Expected status:" + upperStatus
            + " actual status " + newStatus,
            upperStatus.getLen(),
            newStatus.getLen());
  }

}
