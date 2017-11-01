package com.ibm.stocator.fs.cos.unittests;

import java.util.HashMap;
import java.util.Locale;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.ibm.stocator.fs.cos.COSAPIClient;

import static com.ibm.stocator.fs.common.Utils.lastModifiedAsLong;

@RunWith(PowerMockRunner.class)
public class COSAPIClientTest {

  private COSAPIClient mCOSAPIClient;
  private HashMap mHashMap;
  private String mBucketName;

  @Before
  public final void before() {
    mCOSAPIClient = PowerMockito.mock(COSAPIClient.class);
    mBucketName = "aa-bb-cc";
    mHashMap = PowerMockito.spy(new HashMap<String, Boolean>());

    Whitebox.setInternalState(mCOSAPIClient, "mCachedSparkJobsStatus", mHashMap);
    Whitebox.setInternalState(mCOSAPIClient, "mCachedSparkOriginated", mHashMap);
    Whitebox.setInternalState(mCOSAPIClient, "mBucket", mBucketName);
  }

  @Test
  public void extractUnifiedObjectNameTest() throws Exception {
    String objectUnified = "a/b/c/gil.data";

    String input = objectUnified;
    String result = Whitebox.invokeMethod(mCOSAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            input, result);

    input = objectUnified + "/_SUCCESS";
    result = Whitebox.invokeMethod(mCOSAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name with _SUCCESS",
            objectUnified, result);

    input = objectUnified + "/"
        + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv-"
        + "attempt_201603171328_0000_m_000000_1";
    result = Whitebox.invokeMethod(mCOSAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name with attempt",
            objectUnified, result);

    input = "a/b/c/gil.data/"
        + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv-"
        + "attempt_20160317132a_wrong_0000_m_000000_1";
    result = Whitebox.invokeMethod(mCOSAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name with wrong taskAttemptID",
            input, result);
  }

  @Test
  public void nameWithoutTaskIDTest() throws Exception {
    String objectName = "a/b/c/gil.data/"
            + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv";

    String input = objectName;
    input = objectName
            + "-attempt_201603171328_0000_m_000000_1";
    String result = Whitebox.invokeMethod(mCOSAPIClient, "nameWithoutTaskID", input);
    Assert.assertEquals("nameWithoutTaskID() shows incorrect name",
            objectName, result);

    input = objectName
            + "attempt_20160317132a_wrong_0000_m_000000_1";
    result = Whitebox.invokeMethod(mCOSAPIClient, "nameWithoutTaskID", input);
    Assert.assertEquals("nameWithoutTaskID() shows incorrect name with wrong taskAttemptID",
            input, result);
  }

  @Test
  public void getMergedPathTest() throws Exception {
    String hostName = "cos://aa-bb-cc.lvm/";
    String hostName2 = "cos://aa-bb-cc-dd.lvm/";
    String pathName = "data7-1-23-a.txt";
    String objectName = "data7-1-23-a.txt/part-00002-attempt_201612062056_0000_m_000002_2";

    //test when the objectName passed in is the same as the pathName
    String result = Whitebox.invokeMethod(mCOSAPIClient, "getMergedPath",
            hostName, new Path(hostName + pathName), pathName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the object name is the same as the path name",
            hostName + pathName, result);

    //test when the objectName starts with the pathName
    result = Whitebox.invokeMethod(mCOSAPIClient, "getMergedPath",
            hostName, new Path(hostName + pathName), objectName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the object name starts with the path name",
            hostName + objectName, result);

    //test when the objectName is not the same, nor does start with, the pathName
    result = Whitebox.invokeMethod(mCOSAPIClient, "getMergedPath",
            hostName, new Path(hostName + pathName), "data7-1-23-a.txt");
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the object name is not the same, nor does start with, the path name",
            hostName + pathName, result);

    //test when the hostName is not part of the Path
    result = Whitebox.invokeMethod(mCOSAPIClient, "getMergedPath",
            hostName, new Path(pathName), objectName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the host name is not part of the Path",
            hostName + objectName, result);

    //test when a different hostName is part of the Path
    result = Whitebox.invokeMethod(mCOSAPIClient, "getMergedPath",
            hostName, new Path(hostName2 + pathName), objectName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when a different host name is part of the Path",
            hostName + objectName, result);
  }

  @Test
  public void getLastModifiedTest() throws Exception {
    String stringTime = "Fri, 06 May 2016 03:44:47 GMT";
    long longTime = 1462506287000L;

    // test to see if getLastModified parses date with default locale
    long result = lastModifiedAsLong(stringTime);
    Assert.assertEquals("getLastModified() shows incorrect time",
            longTime, result);

    // test to see if getLastModified parses date when default locale is different than US
    Locale originalLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.ITALIAN);
      result = lastModifiedAsLong(stringTime);
      Assert.assertEquals("getLastModified() shows incorrect time",
              longTime, result);
    } finally {
      Locale.setDefault(originalLocale);
    }
  }
}
