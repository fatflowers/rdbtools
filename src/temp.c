#include <unistd.h>
#include <stdio.h>
int main(int argc, char * argv[])
{
   int aflag=0, bflag=0, cflag=0;
   int ch;
printf("optind:%d，opterr：%d\n",optind,opterr);
printf("--------------------------\n");
   while ((ch = getopt(argc, argv, "ab:c:de::")) != -1)
   {
       printf("optind: %d,argc:%d,argv[%d]:%s\n", optind,argc,optind,argv[optind]);
       switch (ch) {
       case 'a':
           printf("HAVE option: -a\n\n");
    
           break;
       case 'b':
           printf("HAVE option: -b\n");
         
           printf("The argument of -b is %s\n\n", optarg);
           break;
       case 'c':
           printf("HAVE option: -c\n");
           printf("The argument of -c is %s\n\n", optarg);

           break;
   case 'd':
      printf("HAVE option: -d\n");
      break;
   case 'e':
      printf("HAVE option: -e\n");
      printf("The argument of -e is %s\n\n", optarg);
      break;

       case '?':
           printf("Unknown option: %c\n",(char)optopt);
           break;
       }
   }
   printf("----------------------------\n");
   printf("optind=%d,argv[%d]=%s\n",optind,optind,argv[optind]);
}
