
import java.sql.Connection;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import com.mysql.jdbc.CallableStatement;

//test pull request

public class PaymentProcessor {
	
private static final Boolean False = null;
private static double PMT(double rate,double term,double financeAmount)
 {
     double v = (1+(rate/12)); 
     double t = (-(term/12)*12); 
     double result=(financeAmount*(rate/12))/(1-Math.pow(v,t));
     return result;
 }
 public static void main(String[] args) {
  Connection conn = null;
  Statement stmt = null;
  PreparedStatement pstmt=null;
  PreparedStatement update_pstmt=null;
  ResultSet rs = null;
  String url = null;
  String user = null;
  String password = null;
  String sql = null;
  Calendar cal=null;
  double monthly_pmt;
  double interest=0.0;
  double principle=0.0;
  double ending_balance=0.0;
  double beginning_balance=0.0;
  String truncate_sql=null; 
  Boolean newBatchUpdate=true;
  final int batchSize = 1000;
  int count = 0;
  
  try {
   Class.forName("com.mysql.jdbc.Driver"); //
   } catch (ClassNotFoundException e) {
   System.out.println("driver error");
   e.printStackTrace();
   }
  try {
   url =
    //"jdbc:mysql://localhost/delinq_db?useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
   "jdbc:mysql://localhost/delinq_db?profileSQL=false&&traceProtocol=false&&paranoid=false";
   //简单写法：url = "jdbc:myqsl://localhost/test(数据库名)? user=root(用户)&password=dfg353434(密码)";
   user = "root";
   password = "";
   conn = DriverManager.getConnection(url,user,password);
  } catch (SQLException e) {
   System.out.println("connection error.");
   e.printStackTrace();
  }
  try {
	  	   
	  //sql = "select loan_No, installments, loan_amt, annual_rate, fst_payment_date, payment_date from loan where loan_No=?";
	  sql = "select loan_No, installments, loan_amt, annual_rate, fst_payment_date, payment_date from loan";
	  String update_sql  = "insert into payment_schedule values(?,?,?,?,?,?,?)";
	  pstmt = conn.prepareStatement(sql);
	  //truncate_sql="truncate payment_schedule;";
	  stmt = conn.createStatement();
	  //stmt.execute(truncate_sql);
	  update_pstmt = conn.prepareStatement(update_sql);
	  conn.setAutoCommit(false);
	     rs = pstmt.executeQuery();
         cal=Calendar.getInstance();
		
	    while(rs.next()){
	       String loan_No = rs.getString("loan_No");
	       double loan_amt = rs.getDouble("loan_amt");
	       double annual_rate = rs.getDouble("annual_rate");
	       int payment_date = rs.getInt("payment_date");
	       int installments = rs.getInt("installments");
	       Date fst_pyment_date = rs.getDate("fst_payment_date");
	       Date p_date = fst_pyment_date;
	       cal.setTime(p_date); 
    	   cal.set(Calendar.DAY_OF_MONTH, payment_date); 
    	   cal.roll(Calendar.MONTH, -1); 
    	   p_date= cal.getTime(); 
	       /*System.out.println("the loan_No is " + loan_No);
	       System.out.println("the loan_amt is " + loan_amt);
	       System.out.println("the payment_date is " + payment_date);
	       System.out.println("the installments is " + installments);
	       System.out.println("the fst_pyment_date is " + fst_pyment_date);*/
	       monthly_pmt=PMT(annual_rate, installments, loan_amt );
	       beginning_balance=loan_amt;
	       for (int i=0; i<installments; i++){
	    	   if (newBatchUpdate==false){
	    	   beginning_balance=ending_balance;}
	    	   cal.setTime(p_date); 
	    	   cal.set(Calendar.DAY_OF_MONTH, payment_date); 
	    	   cal.roll(Calendar.MONTH, 1); 
	    	   p_date= cal.getTime(); 
	    	   interest= beginning_balance*annual_rate/12; 
	    	   principle=monthly_pmt-interest;
	    	   ending_balance=beginning_balance-principle;
	    	   update_pstmt.setString(1,loan_No);	    	   
	    	   java.sql.Date d=new java.sql.Date(p_date.getTime());
	    	   update_pstmt.setDate(2, d);
	    	   update_pstmt.setDouble(3,monthly_pmt);
	    	   update_pstmt.setDouble(4,interest);
	    	   update_pstmt.setDouble(5,principle);
	    	   update_pstmt.setDouble(6,beginning_balance);
	    	   update_pstmt.setDouble(7,ending_balance);
	    	   update_pstmt.addBatch();	    	   
	    	   if (i==installments-1) {newBatchUpdate=true;	       	 
	    	   } else {newBatchUpdate=false;}
	    	 //Create an int[] to hold returned values
	    	   if(++count % batchSize == 0) {
	    	   int[] cnt = update_pstmt.executeBatch();
	    	   }
	    	   //Explicitly commit statements to apply changes
	    	   conn.commit();
	       }
	    }
     /*
   stmt = conn.createStatement();
   java.sql.CallableStatement cstmt = null;
   try {
      String SQL = "{call getEmpName (?, ?)}";
      cstmt = conn.prepareCall (SQL);
   }
   */
  }
   catch (Exception e) {
	  e.printStackTrace();
   }
   finally {
	   try {
		update_pstmt.executeBatch();
		conn.commit();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
  try {
   if(rs != null) {
    rs.close();
    rs = null;
   }
   if(update_pstmt != null) {
	   update_pstmt.close();
	   update_pstmt = null;
   }
   if(conn != null) {
    conn.close();
    conn = null;
   }
  } catch(Exception e) {
   System.out.println("数据库关闭错误");
   e.printStackTrace();
  }
 }
}
