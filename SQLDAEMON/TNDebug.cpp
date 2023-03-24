/****************************************************************************

	���ϸ� : TNDebug.h

	�ۼ��� : �����(spencerj@korea.com)
	�ۼ��� : 2003-11-22

	Tab size : 4 spaces
	������Ʈ�� : Tantra


	���� : 
		-  
		- 

****************************************************************************/

#include "TNDebug.h"


void WriteLog( char* pLog, char* chFileName )
{	
	HANDLE		hFile;
	DWORD		dwRetWrite;
	int			nBufLength;

	CreateDirectory( "\\Data", NULL) ;
	nBufLength	= strlen(pLog);
	hFile = CreateFile(chFileName,					  // file name
                                GENERIC_WRITE|GENERIC_READ,   // open for writing 
                                FILE_SHARE_WRITE,             // do not share 
                                NULL,                         // no security 
                                OPEN_ALWAYS,                  // open or create 
                                FILE_ATTRIBUTE_NORMAL,        // normal file 
                                NULL);

	if(hFile == INVALID_HANDLE_VALUE)
	{
	}
	else
	{
		SetFilePointer(hFile, 0, NULL, FILE_END);
		WriteFile(hFile, pLog, nBufLength, &dwRetWrite, NULL);
		CloseHandle(hFile);
	}
}


