/*
cd src/uk/ac/bris/cs/databases/cwk3
cd ../../../../../../..

ANT ON UNIX - Note for Alex
export ANT_HOME=/Library/Ant
export PATH=${PATH}:${ANT_HOME}/bin
*/

package uk.ac.bris.cs.databases.cwk3;

import java.sql.*;
//import java.util.List;
import java.util.*;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.AdvancedForumSummaryView;
import uk.ac.bris.cs.databases.api.AdvancedForumView;
import uk.ac.bris.cs.databases.api.ForumSummaryView;
import uk.ac.bris.cs.databases.api.ForumView;
import uk.ac.bris.cs.databases.api.AdvancedPersonView;
import uk.ac.bris.cs.databases.api.PostView;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.PersonView;
import uk.ac.bris.cs.databases.api.SimpleForumSummaryView;
import uk.ac.bris.cs.databases.api.SimplePostView; //ALEX JUST ADDED THIS - SHOULD WE NEED TO BE DOING THIS?
import uk.ac.bris.cs.databases.api.SimpleTopicView;
import uk.ac.bris.cs.databases.api.TopicView;
import uk.ac.bris.cs.databases.util.Params;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

    public API(Connection c) {
        this.c = c;
    }

   //Test with: http://localhost:8000/people
   @Override
   public Result<Map<String, String>> getUsers() {
   	try(
   		PreparedStatement s = c.prepareStatement(
               "SELECT username, name FROM Person;"
   		);
         ){
            Map<String, String> map = new HashMap<>();
            ResultSet r = s.executeQuery();
            while(r.next()){
               map.put(r.getString("username"), r.getString("name"));
            }
            return Result.success(map);
   	}catch (SQLException ex) {
         printError("Error in getUsers: " + ex.getMessage());
   	}
   	return Result.fatal("Fatal getUsers");
   }

   //Test with: http://localhost:8000/person/tb15269
   @Override
   public Result<PersonView> getPersonView(String username) {
      Params.cannotBeEmpty(username);
      Params.cannotBeNull(username);
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT name, username, stuId FROM Person " + "WHERE username = ?"
            );
         ){
         s.setString(1, username);
         ResultSet r = s.executeQuery();
         PersonView pv = new PersonView(r.getString("name"),
                                        r.getString("username"),
                                        r.getString("stuId"));
         return Result.success(pv);
      }catch (SQLException ex) {
         printError("Error in getPersonView: " + ex.getMessage());
      }
      return Result.fatal("Fatal getPersonView");
   }

    //Test with: http://localhost:8000/forums0
    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT id, name FROM Forum;"
            );
         ){
         ResultSet r = s.executeQuery();
         List simpleForumsList = new ArrayList<SimpleForumSummaryView>();
         while (r.next()) {
            SimpleForumSummaryView sfsv = new SimpleForumSummaryView(r.getLong("id"),
                                                                     r.getString("name"));
            simpleForumsList.add(sfsv);
         }
         return Result.success(simpleForumsList);
      }catch (SQLException ex) {
         printError("Error in getSimpleForums: " + ex.getMessage());
      }
      return Result.fatal("Fatal getSimpleForums");
    }

    @Override
    public Result<Integer> countPostsInTopic(long topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<PersonView>> getLikers(long topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    // Test with: http://localhost:8000/topic0/1
    @Override
    public Result<SimpleTopicView> getSimpleTopic(long topicId) {
      try(
            PreparedStatement s = c.prepareStatement(
                                 // may not need p.id
               "SELECT t.id as topicid, t.title, p.id as postid, per.username, p.text, p.date FROM Topic AS t " +
               "JOIN Post AS p ON t.id = p.topicid " +
               "JOIN Person AS per ON p.authorid = per.id " +
               "WHERE t.id = ?"
            );
         ){
         s.setLong(1, topicId);

         ResultSet r = s.executeQuery();
         
         String topicTitle = r.getString("title");
         Long topicID = r.getLong("topicid");
         
         //Collect post and add to list
         List<SimplePostView> simplePostsList = new ArrayList<>();
         while (r.next()) {              
            SimplePostView spv = new SimplePostView(r.getInt("postid"),
                                                    r.getString("username"),
                                                    r.getString("text"),
                                                    r.getInt("date"));
            simplePostsList.add(spv);
         }
   
         //Create simpleTopicView pass list of posts
         SimpleTopicView stv = new SimpleTopicView(topicID,
                                                   topicTitle,
                                                   simplePostsList);

         return Result.success(stv);

      }catch (SQLException ex) {
         printError("Error in getSimpleTopic: " + ex.getMessage());
      }
      return Result.fatal("Fatal getSimpleTopic");
    }

    @Override
    public Result<PostView> getLatestPost(long topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<ForumSummaryView>> getForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createForum(String title) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createPost(long topicId, String username, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<ForumView> getForum(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<TopicView> getTopic(long topicId, int page) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result likeTopic(String username, long topicId, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result favouriteTopic(String username, long topicId, boolean fav) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createTopic(long forumId, String username, String title, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result likePost(String username, long topicId, int post, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void printError(String s){
       System.err.println(s);
    }

    private void printDebug(String s){
       System.out.println("\\x1b[32m" + s + "\\x1b[0m");
    }

   }
