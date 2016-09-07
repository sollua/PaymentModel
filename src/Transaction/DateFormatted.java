package Transaction;

import java.util.Date;

public class DateFormatted implements Comparable<DateFormatted>{
	Date dueOrPaidDate;
	String dueOrPaidProperty;
	double positionAtPaymentAxel;
	DateFormatted(Date dueOrPaidDate, String dueOrPaidProperty, double positionAtPaymentAxel){
		this.dueOrPaidDate=dueOrPaidDate;
		this.dueOrPaidProperty=dueOrPaidProperty;	
		this.positionAtPaymentAxel=positionAtPaymentAxel;
	}
	@Override
	public int compareTo(DateFormatted o) {
		if (this.positionAtPaymentAxel<o.positionAtPaymentAxel)
			return -1;
		else 
		return 1;
	}

}

class Subtraction {
	int subtractor;
	int subtracted;
	String PARStatus;

	Subtraction() {
		this.subtractor = 0;
		this.subtracted = 0;
	}

	void setSubtractor(int subtractor) {
		this.subtractor = subtractor;
	}

	void setSubtracted(int subtracted) {
		this.subtracted = subtracted;
	}

	void subtract() {
		switch (subtracted - subtractor) {
		case 0:
			PARStatus= "Current";
			break;
		case 1:
			PARStatus=  "1MD";
			break;
		case 2:
			PARStatus=  "2MD";
			break;
		case 3:
			PARStatus=  "3MD";
			break;
		default:
			PARStatus=  "3MDPlus";
			break;
		}
	}

	int getSubtracted() {
		// TODO Auto-generated method stub
		return subtracted;
	}
		int getSubtractor() {
			// TODO Auto-generated method stub
			return subtractor;
	}

		public String getStatus() {
			// TODO Auto-generated method stub
			return PARStatus;
		}
}
	