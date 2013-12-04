/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.unit.common.lucene;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.Lucene;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * 
 */
public class LuceneTest {


    /*
     * simple test that ensures that we bumb the version on Upgrade
     */
    @Test
    public void testVersion() {
        ESLogger logger = ESLoggerFactory.getLogger(LuceneTest.class.getName());
        Version[] values = Version.values();
        assertThat(Version.LUCENE_CURRENT, equalTo(values[values.length-1]));
        assertThat("Latest Lucene Version is not set after upgrade", Lucene.VERSION, equalTo(values[values.length-2]));
        assertThat(Lucene.parseVersion(null, Lucene.VERSION, null), equalTo(Lucene.VERSION));
        for (int i = 0; i < values.length-1; i++) {
            // this should fail if the lucene version is not mapped as a string in Lucene.java
            assertThat(Lucene.parseVersion(values[i].name().replaceFirst("^LUCENE_(\\d)(\\d)$", "$1.$2"), Version.LUCENE_CURRENT, logger), equalTo(values[i]));
        }
    }
    
    @Test
    public void testTopDocsSerialization()
    {
        ScoreDoc[] scoreDocs=new ScoreDoc[2];
        scoreDocs[0]=new ScoreDoc(456,.678f,0);
        scoreDocs[1]=new ScoreDoc(123,.345f,0);
        TopDocs topDocs=new TopDocs(2,scoreDocs,.678f);
        testTopDocsSerialization(topDocs);
        
        //No hits
        testTopDocsSerialization(new TopDocs(0,new ScoreDoc[0],0.0f));
        
        //This would never happen, we'd send a 0 length array
        //testTopDocsSerialization(new TopDocs(0,null,0.0f));  
    }
    
    @Test
    public void testTopFieldDocsSerialization()
    {
        //Hits, sort by integer field
        FieldDoc[] scoreDocs=new FieldDoc[2];
        scoreDocs[0]=new FieldDoc(456,.678f,new Object[]{new Integer(90)},0);
        scoreDocs[1]=new FieldDoc(123,.345f,new Object[]{new Integer(40)},0);
        SortField[] sortFields=new SortField[]{new SortField("field",SortField.Type.INT)};
        TopFieldDocs topDocs=new TopFieldDocs(2,scoreDocs,sortFields,.678f);
        testTopDocsSerialization(topDocs);
        
        //Hits, sort by integer field, no value in integer field
        scoreDocs=new FieldDoc[2];
        scoreDocs[0]=new FieldDoc(456,.678f,null,0);
        scoreDocs[1]=new FieldDoc(123,.345f,null,0);
        sortFields=new SortField[]{new SortField("field",SortField.Type.INT)};
        topDocs=new TopFieldDocs(2,scoreDocs,sortFields,.678f);
        testTopDocsSerialization(topDocs);
        
        //No hits
        testTopDocsSerialization(new TopFieldDocs(0,new ScoreDoc[0],sortFields,0.0f));
        testTopDocsSerialization(new TopFieldDocs(0,new ScoreDoc[0],new SortField[0],0.0f));
    }
    
    private void testTopDocsSerialization(TopDocs topDocs)
    {
        TopDocs topDocs2=null;
        try
        {
            BytesStreamOutput out = new BytesStreamOutput();
            Lucene.writeTopDocs(out, topDocs, 0);
            
            BytesStreamInput in = new BytesStreamInput(out.bytes().toBytes(), false);
            topDocs2=Lucene.readTopDocs(in);
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        assertThat("Object did not deserialize", topDocs2!=null);
        assertThat("totalHits doesn;t match",topDocs.totalHits==topDocs2.totalHits);
        assertThat("maxScore doesn't match",topDocs.getMaxScore()==topDocs2.getMaxScore());
        assertScoreDocs(topDocs.scoreDocs,topDocs2.scoreDocs);
    }
    
    @Test
    public void testTopGroupsSerialization()
    {
        //Hits, grouped by string field, groups sorted by score, hits sorted by score
        SortField[] groupSort=new SortField[]{SortField.FIELD_SCORE};
        SortField[] withinGroupSort=new SortField[]{SortField.FIELD_SCORE};
        GroupDocs<String>[] groups=new GroupDocs[2];
        
        ScoreDoc[] scoreDocsg1=new ScoreDoc[2];
        scoreDocsg1[0]=new ScoreDoc(456,.9f,0);
        scoreDocsg1[1]=new ScoreDoc(123,.345f,0);
        groups[0]=new GroupDocs<String>(2.0f,.9f,2,scoreDocsg1,"bond",null);
        
        ScoreDoc[] scoreDocsg2=new ScoreDoc[2];
        scoreDocsg2[0]=new ScoreDoc(54,.8f,0);
        scoreDocsg2[1]=new ScoreDoc(78,.45f,0);
        groups[1]=new GroupDocs<String>(2.0f,.8f,2,scoreDocsg2,"option",null);
        
        TopGroups<String> topGroupsTemp=new TopGroups<String>(groupSort,withinGroupSort,10,5,groups,.9f);
        TopGroups<String> topGroups=new TopGroups<String>(topGroupsTemp,new Integer(3));
        testTopGroupsSerialization(topGroups);
    }
    
    private <T> void testTopGroupsSerialization(TopGroups<T> topGroups1)
    {
        TopGroups<T> topGroups2=null;
        try
        {
            BytesStreamOutput out = new BytesStreamOutput();
            Lucene.writeTopGroups(out, topGroups1, 0,0);
            
            BytesStreamInput in = new BytesStreamInput(out.bytes().toBytes(), false);
            topGroups2=Lucene.readTopGroups(in);
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        assertThat("Object did not deserialize", topGroups2!=null);
        assertThat("totalHitCount doesn't match",topGroups1.totalHitCount==topGroups2.totalHitCount);
        assertThat("totalGroupedHitCount doesn't match",topGroups1.totalGroupedHitCount==topGroups2.totalGroupedHitCount);
        assertThat("totalGroupCount doesn't match",topGroups1.totalGroupCount==topGroups2.totalGroupCount||
                topGroups1.totalGroupCount.equals(topGroups2.totalGroupCount));
        
        assertThat("groups is different length",topGroups1.groups.length==topGroups2.groups.length);
        for(int ii=0;ii<topGroups1.groups.length;ii++)
        {
            assertGroupDocs(topGroups1.groups[ii],topGroups2.groups[ii]);
        }
        
        assertThat("maxScore doesn't match",topGroups1.maxScore==topGroups2.maxScore);
    }
    
    private <T> void assertGroupDocs(GroupDocs<T> groupDocs1,GroupDocs<T> groupDocs2)
    {
        assertThat("GroupDocs.groupValue doesn't match",groupDocs1.groupValue.equals(groupDocs2.groupValue));
        assertThat("GroupDocs.maxScore doesn't match",groupDocs1.maxScore==groupDocs2.maxScore);
        assertThat("GroupDocs.score doesn't match",groupDocs1.score==groupDocs2.score);        
        assertScoreDocs(groupDocs1.scoreDocs,groupDocs2.scoreDocs);
        assertThat("GroupDocs.totalHits doesn't match",groupDocs1.totalHits==groupDocs2.totalHits);
        
        assertThat("GroupDocs.groupSortValues.length doesn't match",groupDocs1.groupSortValues==groupDocs2.groupSortValues||groupDocs1.groupSortValues.length==groupDocs2.groupSortValues.length);
        if(groupDocs1.groupSortValues!=null)
        {
            for(int ii=0;ii<groupDocs1.groupSortValues.length;ii++)
            {
                assertThat("GroupDocs.groupSortValues["+ii+"] doesn't match",groupDocs1.groupSortValues[ii].equals(groupDocs2.groupSortValues[ii]));
            }
        }
    }
    
    private void assertScoreDocs(ScoreDoc[] docs1, ScoreDoc[] docs2)
    {
        assertThat("ScoreDoc[].length doesn't match",docs1.length==docs2.length);
        for(int ii=0;ii<docs1.length;ii++)
        {
            assertScoreDocs(docs1[ii],docs2[ii]);
        }
    }
    
    private void assertScoreDocs(ScoreDoc doc1, ScoreDoc doc2)
    {
        assertThat("ScoreDoc.doc  doesn't match",doc1.doc==doc2.doc);
        assertThat("ScoreDoc.score doesn't match",doc1.score==doc2.score);
//        assertThat("ScoreDoc.shardIndex doesn't match",doc1.shardIndex==doc2.shardIndex);
        boolean sameClass=doc1 instanceof FieldDoc==doc2 instanceof FieldDoc;
        assertThat("ScoreDoc's are not the same class",sameClass);
        if(sameClass && doc1 instanceof FieldDoc)
        {
            FieldDoc field1=(FieldDoc)doc1;
            FieldDoc field2=(FieldDoc)doc2;
            assertThat("FieldDoc.fields.length doesn't match",field1.fields==field2.fields||field1.fields.length==field2.fields.length);
            if(field1.fields!=null)
            {
                for(int ii=0;ii<field1.fields.length;ii++)
                {
                    assertThat("FieldDoc.fields["+ii+"] doesn't match",field1.fields[ii].equals(field2.fields[ii]));
                }
            }
        }
    }
}
