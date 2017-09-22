//-----------------------------------------------------------------------
// Original work Copyright (c) 2011, Oliver M. Haynold
// Modified work Copyright (c) 2013, Suraj Gupta
// Modified work Copyright (c) 2015, Atif Aziz
// All rights reserved.
//-----------------------------------------------------------------------

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace RserveCLI2
{
    /// <summary>
    /// An implementation of the QAP1 protocol used to communicate with Rserve
    /// </summary>
    internal class Qap1
    {

        #region Constants and Fields

        #region DT_ declarations

        /// <summary>
        /// Int data
        /// </summary>
        internal const byte DtInt = 1;

        /// <summary>
        /// String data
        /// </summary>
        internal const byte DtString = 4;

        /// <summary>
        /// Byte stream data
        /// </summary>
        internal const byte DtByteStream = 5;

        /// <summary>
        /// The data stream containts a Sexp
        /// </summary>
        internal const byte DtSexp = 10;

        /// <summary>
        /// Large data flag
        /// </summary>
        internal const byte DtLarge = 64;

        #endregion

        #region XT_ declarations

        /// <summary>
        /// The Sexp is NULL
        /// </summary>
        internal const byte XtNull = 0;

        /// <summary>
        /// S4 object Sexp
        /// </summary>
        internal const byte XtS4 = 7;

        /// <summary>
        /// Vector Sexp
        /// </summary>
        internal const byte XtVector = 16;

        /// <summary>
        /// Closure Sexp
        /// </summary>
        internal const byte XtClos = 18;

        /// <summary>
        /// Symbol name Sexp
        /// </summary>
        internal const byte XtSymName = 19;

        /// <summary>
        /// List without tags
        /// </summary>
        internal const byte XtListNoTag = 20;

        /// <summary>
        /// List with tags
        /// </summary>
        internal const byte XtListTag = 21;

        /// <summary>
        /// Lang without tags
        /// </summary>
        internal const byte XtLangNoTag = 22;

        /// <summary>
        /// Lang with tags
        /// </summary>
        internal const byte XtLangTag = 23;

        /// <summary>
        /// Vector expression
        /// </summary>
        internal const byte XtVectorExp = 26;

        /// <summary>
        /// Vector string
        /// </summary>
        internal const byte XtVectorStr = 27;

        /// <summary>
        /// Array of integers
        /// </summary>
        internal const byte XtArrayInt = 32;

        /// <summary>
        /// Array of doubles
        /// </summary>
        internal const byte XtArrayDouble = 33;

        /// <summary>
        /// Array of strings
        /// </summary>
        internal const byte XtArrayString = 34;

        /// <summary>
        /// Array of Bool UA
        /// </summary>
        internal const byte XtArrayBoolUa = 35;

        /// <summary>
        /// Array of Bool
        /// </summary>
        internal const byte XtArrayBool = 36;

        /// <summary>
        /// The Sexp contains raw data
        /// </summary>
        internal const byte XtRaw = 37;

        /// <summary>
        /// Array of complex
        /// </summary>
        internal const byte XtArrayComplex = 38;

        /// <summary>
        /// Unknown data type
        /// </summary>
        internal const byte XtUnknown = 48;

        /// <summary>
        /// The length of the Sexp is coded as a 56-bit integer, enlarging the header by 4 bytes
        /// </summary>
        internal const byte XtLarge = 64;

        /// <summary>
        /// Flag for the presence of attributes
        /// </summary>
        internal const byte XtHasAttr = 128;

        #endregion

        #region Errors

        // stat codes; 0-0x3f are reserved for program specific codes - e.g. for R
        // connection they correspond to the stat of Parse command.
        // the following codes are returned by the Rserv itself
        // codes < 0 denote Rerror as provided by R_tryEval

        /// <summary>
        /// auth.failed or auth.requested but no login came.
        /// in case of authentification failure due to name/pwd mismatch,
        /// server may send CMD_accessDenied instead
        /// </summary>
        internal const byte ErrAuthFailed = 0x41;

        /// <summary>
        /// connection closed or broken packet killed it
        /// </summary>
        internal const byte ErrConnBroken = 0x42;

        /// <summary>
        /// unsupported/invalid command
        /// </summary>
        internal const byte ErrInvCmd = 0x43;

        /// <summary>
        /// Some parameters are invalid
        /// </summary>
        internal const byte ErrInvPar = 0x44;

        /// <summary>
        /// R-error occured, usually followed by connection shutdown
        /// </summary>
        internal const byte ErrRerror = 0x45;

        /// <summary>
        /// I/O error
        /// </summary>
        internal const byte ErrIoError = 0x46;

        /// <summary>
        /// Attempt to perform fileRead/Write on closed file.
        /// </summary>
        internal const byte ErrNotOpen = 0x47;

        /// <summary>
        /// This answer is also valid onC MD_login; otherwise it's sent if the server doesn't
        /// allow the user to issue the specified command.
        /// (e.g. some server admins may block file I/O operations for some users)
        /// </summary>
        internal const byte ErrAccessDenied = 0x48;

        /// <summary>
        /// Unsupported command
        /// </summary>
        internal const byte ErrUnsupportedCmd = 0x49;

        /// <summary>
        /// unknown command - the difference between unsupported and unknown is that unsupported commands
        /// are known to the server but for some reasons (e.g. platform dependent) it's not supported.
        /// unknown commands are simply not recognized by the server at all.
        /// </summary>
        internal const byte ErrUnknownCmd = 0x4a;

        /// <summary>
        /// incoming packet is too big. currently there is a limit as of the size of an incoming packet.
        /// </summary>
        internal const byte ErrDataOverflow = 0x4b;

        /// <summary>
        /// The requested object is too big to be transported in that way.
        /// If received after CMD_eval then the evaluation itself was successful.
        /// Optional parameter is the size of the object
        /// </summary>
        internal const byte ErrObjectTooBig = 0x4c;

        /// <summary>
        /// Out of memory. the connection is usually closed after this error was sent.
        /// </summary>
        internal const byte ErrOutOfMem = 0x4d;

        /// <summary>
        /// control pipe to the master process is closed or broken
        /// </summary>
        internal const byte ErrCtrlClosed = 0x4e;

        /// <summary>
        /// session is still busy
        /// </summary>
        internal const byte ErrSessionBusy = 0x50;

        /// <summary>
        /// Unable to detach seesion (cannot determine peer IP or problems creating a listening socket for resume)
        /// </summary>
        internal const byte ErrDetachFailed = 0x51;

        /// <summary>
        /// feature is disabled
        /// </summary>
        internal const byte ErrDisabled = 0x61;

        /// <summary>
        /// feature is not present in this build
        /// </summary>
        internal const byte ErrUnavailable = 0x62;

        /// <summary>
        /// crypto-system error
        /// </summary>
        internal const byte ErrCryptError = 0x63;

        /// <summary>
        /// server-initiated close due to security violation (too many attempts, excessive timeout etc.)
        /// </summary>
        internal const byte ErrSecurityClose = 0x64;

        #endregion

        #endregion

        #region Public Methods

        /// <summary>
        /// Initializes a new instance of the Qap1 class.
        /// </summary>
        /// <param name="socket">The socket through which we'll communicate with the server.</param>
        public Qap1( Socket socket )
        {
            if ( !BitConverter.IsLittleEndian )
            {
                throw new PlatformNotSupportedException( "As of now, this works only on little-endian machines. " +
                    "If you have access to CLI on a big-endian machine, feel free to check whether it works and remove this check." );
            }

            _socket = socket;
        }

        /// <summary>
        /// Send a command and read the stream it returns (used for reading files)
        /// </summary>
        /// <param name="cmd">Command to be sent to the server</param>
        /// <param name="data">Arguments for the command</param>
        /// <returns>The data read</returns>
        public async Task<byte[]> CommandReadStreamAsync(int cmd, IList<object> data)
        {
            long toConsume = await SubmitCommandAsync(cmd, data).ContinueContextFree();
            var res = new byte[ toConsume ];
            int stored = 0;
            int retrieved = -1;

            while ( ( stored < toConsume ) && ( retrieved != 0 ) )
            {
                var receiveSize = Math.Min(DefaultReceiveSize, (int) toConsume - stored);
                retrieved = await _socket.ReceiveAsync(res, stored, receiveSize, SocketFlags.None).ContinueContextFree();
                stored += retrieved;
            }

            if ( stored != toConsume )
            {
                throw new RserveException( "When reading stream from server, client expects " + toConsume + " bytes of data, but received " + stored + "." );
            }

            return res;
        }

        /// <summary>
        /// Send a command and read the result
        /// </summary>
        /// <param name="cmd">Command to be sent to the server</param>
        /// <param name="data">Arguments for the command</param>
        /// <returns>The result, parsed into appropriate objects (string or Sexp)</returns>
        public async Task<List<object>> CommandAsync(int cmd, IList<object> data)
        {
            long toConsume = await SubmitCommandAsync(cmd, data).ContinueContextFree();
            var res = new List<object>();
            var dhbuf = ArrayPool<byte>.Shared.Rent(9);
            while ( toConsume > 0 )
            {
                Array.Clear(dhbuf, 0, 9);

                // pull the first 4 bytes of the header
                // first byte is the DT declaration.  Next three bytes used for length of payload.
                int headerLength = 4;
                if (await _socket.ReceiveAsync(dhbuf, 4).ContinueContextFree() != 4)
                {
                    throw new RserveException( "When receiving command parameter from server, client did not receive the requisite 4-byte header." );
                }

                // is this a large dataset?  if so, pull the next four bytes which are also used for length of payload.
                byte typ = dhbuf[ 0 ];
                if ( ( typ & DtLarge ) == DtLarge )
                {
                    headerLength += 4;
                    if (await _socket.ReceiveAsync(dhbuf, 4, 4, SocketFlags.None).ContinueContextFree() != 4)
                    {
                        throw new RserveException( "When receiving command parameter with large data from server, client did not receive the requisite extra 4-bytes in the header." );
                    }
                }

                // determine length of payload
                var dlength = ( int ) BitConverter.ToUInt64( dhbuf , 1 );

                // pull the payload from the socket
                int receivedTotal = 0;
                var dvbuf = ArrayPool<byte>.Shared.Rent(dlength);
                while ( receivedTotal < dlength )
                {
                    var receiveSize = Math.Min(DefaultReceiveSize, (int) dlength - receivedTotal);
                    var received = await _socket.ReceiveAsync(dvbuf, receivedTotal, receiveSize, SocketFlags.None).ContinueContextFree();
                    if ( received > 0 )
                    {
                        receivedTotal += received;
                    }
                    else
                    {
                        throw new RserveException( "When receiving command parameter from server, the payload is expected to be " + dlength + " bytes of data, but the client received " + receivedTotal + "." );
                    }
                }

                if ( ( typ & DtString ) == DtString )
                {
                    int count = dlength;
                    while ( ( count > 0 ) && ( dvbuf[ count - 1 ] != 0 ) )
                    {
                        count--;
                    }
                    if ( count > int.MaxValue )
                    {
                        throw new RserveException( "Recieved a DTString of length greater than Int32 from the server, which is not supported." );
                    }
                    res.Add( Encoding.UTF8.GetString( dvbuf , 0 , count ) );
                }

                else if ( ( typ & DtSexp ) == DtSexp )
                {
                    int start = 0;
                    res.Add( DecodeSexp( dvbuf , ref start ) );
                }
                else
                {
                    throw new RserveException( "Recieved an unknown command parameter type from the server:" + typ );
                }

                toConsume -= ( headerLength + dlength );
            }

            ArrayPool<byte>.Shared.Return(dhbuf);

            return res;
        }

        #endregion

        #region Private Members

        /// <summary>
        /// The socket used to communicate with Rserve
        /// </summary>
        private readonly Socket _socket;

        private const int DefaultReceiveSize = 8 * 1024;
        private static readonly SexpNull SexpNull = new SexpNull();

        /// <summary>
        /// Submit a command to Rserve
        /// </summary>
        /// <param name="cmd">The command to be submitted to the server</param>
        /// <param name="data">The arguments for the command</param>
        /// <returns>Length of the response in bytes</returns>
        private async Task<long> SubmitCommandAsync( int cmd , IList<object> data )
        {
            // Build command
            var sbuf = new List<byte>();
            foreach ( var a in data )
            {
                List<byte> argbuf;
                byte dt;
                if ( a is string )
                {
                    var bytes = Encoding.UTF8.GetBytes( a as string );
                    argbuf = new List<byte>(bytes.Length + 1 + 3);
                    argbuf.AddRange( bytes );
                    argbuf.Add( 0 ); // string must be null terminated

                    // strings must be padded with zeros so length of the content is divisible by 4
                    while ( argbuf.Count % 4 != 0 )
                    {
                        argbuf.Add( 0 );
                    }

                    dt = DtString;
                }
                else if ( a is Sexp )
                {
                    argbuf = EncodeSexp( a as Sexp );
                    dt = DtSexp;
                }
                else if ( a is byte[] )
                {
                    argbuf = new List<byte>(( byte[] ) a );
                    dt = DtByteStream;
                }
                else if ( a is int )
                {
                    argbuf = new List<byte>( BitConverter.GetBytes( ( int )a ) );
                    dt = DtInt;
                }
                else
                {
                    throw new RserveException( "Attempting to encode an invalid command parameter type." );
                }

                // get payload length
                long len = argbuf.Count;
                byte[] lenBytes = BitConverter.GetBytes( len );

                // populate header (first four bytes)
                int headerSize = 3;

                // a large dataset is > 16MB, it requires the DtLarge flag and an extra 4 bytes in the header to esablish correct payload size
                bool isLargeData = len > 0xfffff0;
                if ( isLargeData )
                {
                    dt |= DtLarge;
                    headerSize = 7;
                }

                // insert header
                sbuf.Add( dt );
                for (int i = 0; i < headerSize; ++i)
                {
                    sbuf.Add( lenBytes[i] );
                }
                sbuf.AddRange( argbuf );
            }

            // header structure:
            // [0]  (int) command - specifies the request or response type
            // [4]  (int) length of the message (bits 0-31) - specifies the number of bytes belonging to this message (excluding the header)
            // [8]  (int) offset of the data part - specifies the offset of the data part, where 0 means directly after the header (which is normally the case)
            // [12] (int) length of the message (bits 32-63) - high bits of the length (must be 0 if the packet size is smaller than 4GB)
            var header = new List<byte>(16);
            long mlen = sbuf.Count;
            byte[] mlenBytes = BitConverter.GetBytes( mlen );

            header.AddRange( BitConverter.GetBytes( cmd ) );
            for (int i = 0; i < 4; ++i)
            {
                header.Add( mlenBytes[i] );
            }
            for (int i = 0; i < 4; ++i)
            {
                header.Add( 0 );
            }
            for (int i = 4; i < mlenBytes.Length; ++i)
            {
                header.Add( mlenBytes[i] );
            }

            // Execute Command
            await _socket.SendAsync( header.ToArray() ).ContinueContextFree();
            await _socket.SendAsync( sbuf.ToArray() ).ContinueContextFree();

            // Read Response
            var hdrbuf = ArrayPool<byte>.Shared.Rent(16);
            if ( await _socket.ReceiveAsync( hdrbuf, 0, 16, SocketFlags.None ).ContinueContextFree() != 16 )
            {
                throw new RserveException( "Response from server does not contain a header." );
            }

            // did the server return an error?
            int cmdResult = BitConverter.ToInt32( hdrbuf , 0 );
            if ( ( cmdResult & 15 ) != 1 )
            {
                int serverErrorCode = ( ( cmdResult >> 24 ) & 127 );
                throw new RserveException( serverErrorCode );
            }

            // not expecting an non-zero offset
            var offset = BitConverter.ToUInt32( hdrbuf , 8 );
            if ( offset != 0 )
            {
                throw new RserveException( "In response from server, offset is not 0." );
            }

            // calculate length of response payload from the header
            ulong length = BitConverter.ToUInt64( hdrbuf , 4 );

            ArrayPool<byte>.Shared.Return(hdrbuf);

            return ( long )length;
        }

        /// <summary>
        /// Encode a Sexp in Qap1 format
        /// </summary>
        /// <param name="s">The Sexp to be encoded</param>
        /// <returns>QAP4-encoded bit stream</returns>
        private static List<byte> EncodeSexp( Sexp s )
        {
            var t = s.GetType();
            List<byte> res = null;
            byte xt;
            SexpTaggedList attrs = null;
            if (s.HasAttributes)
            {
                attrs = new SexpTaggedList();
                foreach ( var a in s.Attributes )
                {
                    attrs.Add( a.Key , a.Value );
                }

                res = EncodeSexp( attrs );
            }
            if ( t == typeof( SexpNull ) )
            {
                res = res ?? new List<byte>(0);
                xt = XtNull;
            }
            else if ( t == typeof( SexpArrayDouble ) )
            {
                xt = XtArrayDouble;
                var v = ( ( SexpArrayDouble )s ).Value;
                res = res ?? new List<byte>( v.Count * 8);
                foreach ( var t1 in v )
                {
                    res.AddRange( BitConverter.GetBytes( t1 ) );
                }
            }
            else if ( t == typeof( SexpArrayInt ) || t == typeof( SexpArrayDate ) )
            {
                xt = XtArrayInt;
                var v = ( ( SexpArrayInt )s ).Value;
                res = res ?? new List<byte>( v.Count * 4 );
                foreach ( var t1 in v )
                {
                    res.AddRange( BitConverter.GetBytes( t1 ) );
                }
            }
            else if ( t == typeof( SexpArrayBool ) )
            {
                xt = XtArrayBool;
                var v = ( SexpArrayBool )s;

                res = res ?? new List<byte>( 4 + v.Count + 3 );
                res.AddRange( BitConverter.GetBytes( v.Count ) );

                // R logical is false if 0, true if 1, and NA if 2
                foreach (var value in v)
                {
                    res.Add(((SexpArrayBool) value).AsByte);
                }

                // protocol requires us to pad with null
                while ( res.Count % 4 != 0 )
                {
                    res.Add( 0 );
                }
            }
            else if ( t == typeof( SexpTaggedList ) )
            {
                xt = XtListTag;
                var v = ( SexpTaggedList )s;
                res = res ?? new List<byte>();
                foreach ( var a in v.AsSexpDictionary )
                {
                    res.AddRange( EncodeSexp( a.Value ) );
                    res.AddRange( EncodeSexp( new SexpSymname( a.Key ) ) );
                }
            }
            else if ( t == typeof( SexpList ) )
            {
                xt = XtVector;
                var v = ( ( SexpList )s ).Value;
                res = res ?? new List<byte>();
                foreach ( var a in v )
                {
                    res.AddRange( EncodeSexp( a ) );
                }
            }
            else if ( t == typeof( SexpArrayString ) )
            {
                xt = XtArrayString;
                var v = ( ( SexpArrayString )s ).Value;
                res = res ?? new List<byte>();
                foreach ( var a in v )
                {
                    // Rserve represents NA strings using 0xff (255).
                    if ( a == null )
                    {
                        res.Add( 255 );
                    }
                    else
                    {
                        var b = Encoding.UTF8.GetBytes( a );

                        // If 0xff occurs in the beginning of a string it should be doubled to avoid misrepresentation.
                        if ( ( b.Length > 0 ) && ( b[ 0 ] == 255 ) )
                        {
                            res.Add( 255 );
                        }

                        res.AddRange( b );
                    }
                    res.Add( 0 );
                }
            }
            else if ( t == typeof( SexpSymname ) )
            {
                xt = XtSymName;
                var v = ( ( SexpSymname )s ).Value;
                res = res ?? new List<byte>(Encoding.UTF8.GetByteCount(v) + 1);
                var b = Encoding.UTF8.GetBytes( v );
                res.AddRange( b );
                res.Add( 0 );
            }
            else
            {
                throw new RserveException( "Cannot encode an unknown Sexp type " + t.GetType().Name );
            }

            if ( attrs != null )
            {
                xt |= XtHasAttr;
            }

            // get payload length
            long len = res.Count;
            byte[] lenBytes = BitConverter.GetBytes( len );

            // populate header (first four bytes)
            int headerLength = 3;

            // a large dataset is > 16MB, it requires the XtLarge flag and an extra 4 bytes in the header to esablish correct payload size
            bool isLargeData = len > 0xfffff0;
            if ( isLargeData )
            {
                xt |= XtLarge;
                headerLength = 7;
            }

            var result = new List<byte>(res.Count + headerLength + 1);
            // insert header
            result.Add( xt );
            for (var i = 0; i < headerLength; i++)
            {
                result.Add(lenBytes[i]);
            }
            result.AddRange(res);
            return result;
        }

        /// <summary>
        /// Decode a Qap1-encoded Sexp
        /// </summary>
        /// <param name="data">The byte stream in which the Sexp is encoded</param>
        /// <param name="start">At which index of data does the Sexp begin?</param>
        /// <returns>The decoded Sexp.</returns>
        private static Sexp DecodeSexp( byte[] data , ref int start )
        {
            // pull sexp type
            byte xt = data[ start ];

            // calculate length of payload
            var lengthBuf = ArrayPool<byte>.Shared.Rent(8);
            Array.Clear(lengthBuf, 0, 8);
            Array.Copy( data , start + 1 , lengthBuf , 0 , 3 );
            start += 4;
            if ( ( xt & XtLarge ) == XtLarge )
            {
                Array.Copy( data , start , lengthBuf , 3 , 4 );
                start += 4;
                xt -= XtLarge;
            }
            var length = ( int )BitConverter.ToUInt64( lengthBuf , 0 );
            ArrayPool<byte>.Shared.Return(lengthBuf);

            // has attributes?  process first
            SexpTaggedList attrs = null;
            if ( ( xt & XtHasAttr ) == XtHasAttr )
            {
                xt -= XtHasAttr;
                int oldstart = start;
                attrs = ( SexpTaggedList )DecodeSexp( data , ref start );
                length -= start - oldstart;
            }

            int end = start + length;
            Sexp result;

            switch ( xt )
            {
                case XtNull:
                    {
                        if ( length != 0 )
                        {
                            throw new RserveException( "Attempting to decode an SexpNull, but it is followed by data when it shouldn't be." );
                        }
                        result = SexpNull;
                    }
                    break;
                case XtSymName:
                    {
                        // keep all characters up to the first null
                        string res = Encoding.UTF8.GetString( data, start, length );
                        var idx = res.IndexOf('\x00');
                        if (idx > 0)
                        {
                            res = res.Substring(0, idx);
                        }
                        result = new SexpSymname( res );
                    }
                    break;
                case XtArrayInt:
                    {
                        var size = length / 4;
                        var res = ArrayPool<int>.Shared.Rent(size);
                        for ( int i = 0 ; i < length ; i += 4 )
                        {
                            res[ i / 4 ] = BitConverter.ToInt32( data , start + i );
                        }

                        // is date or just an integer?
                        if ( attrs != null
                             && attrs.ContainsKey( "class" )
                             && Array.IndexOf(attrs[ "class" ].AsStrings, "Date" ) > -1)
                        {
                            result = new SexpArrayDate( res, size );
                        }
                        else
                        {
                            result = new SexpArrayInt( res, size );
                        }
                        ArrayPool<int>.Shared.Return(res);
                    }
                    break;
                case XtArrayBool:
                    {
                        if ( length < 4 )
                        {
                            throw new RserveException( "Decoding an SexpArrayBool where data doesn't seem to contain a data length field." );
                        }
                        var datalength = BitConverter.ToInt32( data , (int) start );
                        if ( datalength > length - 4 )
                        {
                            throw new RserveException( "Decoding an SexpArrayBool where transmitted data field too short for number of entries." );
                        }

                        var res = ArrayPool<bool?>.Shared.Rent(datalength);
                        for ( int i = 0 ; i < datalength ; i++ )
                        {
                            // R logical is false if 0, true if 1, and NA if 2
                            switch ( data[ start + i + 4 ] )
                            {
                                case 0:
                                    res[ i ] = false;
                                    break;
                                case 1:
                                    res[ i ] = true;
                                    break;
                                case 2:
                                    res[ i ] = null;
                                    break;
                                default:
                                    throw new RserveException( "Decoding an SexpArrayBool and found an element in the array that is not an R bool: " + data[ start + i + 4 ] );
                            }
                        }

                        result = new SexpArrayBool( res, datalength );
                        ArrayPool<bool?>.Shared.Return(res);
                    }
                    break;
                case XtArrayDouble:
                    {
                        var size = length / 8;
                        var res = ArrayPool<double>.Shared.Rent(size);
                        for ( int i = 0 ; i < length ; i += 8 )
                        {
                            res[ i / 8 ] = BitConverter.ToDouble( data , (int) start + i );
                        }

                        // is date or just a double?
                        if ( attrs != null
                             && attrs.TryGetValue( "class", out Sexp clazz )
                             && Array.IndexOf(clazz.AsStrings, "Date") > -1)
                        {
                            var ints = new List<int>(size);
                            for (int i = 0; i < size; ++i)
                            {
                                ints.Add(Convert.ToInt32(res[i]));
                            }
                            result = new SexpArrayDate( ints );
                        }
                        else
                        {
                            result = new SexpArrayDouble( res, size );
                        }
                        ArrayPool<double>.Shared.Return(res);
                    }
                    break;
                case XtArrayString:
                    {
                        var res = new List<string>();
                        int i = 0;
                        for ( int j = 0 ; j < length ; j++ )
                        {
                            if ( data[ start + j ] != 0 )
                            {
                                continue;
                            }

                            if ( ( j == i + 1 ) && ( data[ start + i ] == 255 ) )
                            {
                                res.Add( null );
                            }
                            else
                            {
                                if ( data[ start + i ] == 255 )
                                {
                                    i++;
                                }

                                res.Add( Encoding.UTF8.GetString( data , start + i, j - i ) );
                            }
                            i = j + 1;
                        }

                        result = new SexpArrayString( res );
                    }
                    break;
                case XtListNoTag:
                case XtLangNoTag:
                case XtVector:
                    result = new SexpList();
                    while ( start < end )
                    {
                        result.Add( DecodeSexp( data , ref start ) );
                    }
                    break;
                case XtLangTag:
                case XtListTag:
                    result = new SexpTaggedList();
                    while ( start < end )
                    {
                        Sexp val = DecodeSexp( data , ref start );
                        Sexp key = DecodeSexp( data , ref start );
                        result.Add( key.IsNull ? String.Empty : key.AsString , val );
                    }

                    break;
                case XtRaw:
                    {
                        var d = new byte[ length ];
                        Array.Copy( data , start , d , 0 , length );
                        result = new SexpQap1Raw( xt , d );
                    }
                    break;
                default:
                    throw new RserveException( "Cannot decode an Sexp because the type is not recognized: " + xt );
            }

            if ( start > end )
            {
                throw new RserveException( "When decoding an Sexp, more data consumed than provided." );
            }

            start = end;
            if ( attrs != null )
            {
                foreach ( var a in attrs.AsSexpDictionary )
                {
                    result.Attributes.Add( a.Key , a.Value );
                }
            }

            return result;
        }

        #endregion

    }
}
